package lasige.steeldb.comm;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import lasige.steeldb.client.SteelDBListener;
import lasige.steeldb.jdbc.BFTRowSet;
import lasige.steeldb.jdbc.ResultSetData;

import bftsmart.tom.ServiceProxy;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.Extractor;
import bftsmart.tom.util.TOMUtil;

public class MessageHandlerTrustedMaster {

	private ServiceProxy proxy;
	private boolean transactionReadOnly;
	private LinkedList<byte[]> resHashes;
	private LinkedList<Message> operations;
	private static final String CONFIG_FOLDER = System.getProperty("divdb.folder", "config");
	private static final int FIRST_CLIENT_ID = Integer.valueOf(System.getProperty("divdb.firstclient", "0"));
	private final int clientId;
	private int master;
	private int oldMaster;
	
    private Logger logger = Logger.getLogger("steeldb_client");
	
	public MessageHandlerTrustedMaster(int clientId) {
		clientId = FIRST_CLIENT_ID + clientId;
		proxy = new ServiceProxy(clientId, CONFIG_FOLDER, new BFTComparator(), new BFTExtractor());
		transactionReadOnly = true;
		this.resHashes = new LinkedList<byte[]>();
		this.operations = new LinkedList<Message>();
		this.clientId = clientId;
		master = 0;
		logger.debug("Client " + clientId + "opened a connection. MessageHandler created");
	}
	
	public MessageHandlerTrustedMaster(int clientId, boolean replica) {
		proxy = new ServiceProxy(clientId, CONFIG_FOLDER, new BFTComparator(), new BFTExtractor());
		transactionReadOnly = true;
		this.clientId = clientId;
		master = 0;
//		logger.debug("Opening connection for client " + proxy.getProcessId());
	}

	/**
	 * Sends the message to a group of replicas.
	 * @param m The message object with attributes and the byte array with the contents
	 * of the message.
	 * @param autocommitIsOff Indicates if the connection that sent the message is in
	 * autocommit mode or not. It is used to determine for which replicas the message
	 * must be sent.
	 * If the connection is in autocoomit mode, the message should be sent to all replicas
	 * in total order. If it is not in autocommit mode the replicas for which the messages
	 * will be send depends on the message type. If it is a read message it must be sent
	 * to two replicas. If it is a write message is must be sent to all replicas but will
	 * only be executed at the master.
	 * @return The message with the results from the execution in the replicas compared
	 * to garantee that the replicas are correct.
	 */
	public synchronized Message send(Message m, boolean autoCommit) {
		try {
			m.setClientId(clientId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		int opCode = m.getOpcode();
		logger.debug("---- Client " + clientId + ", OpCode: " + m.getOpcode() + ", Msg: " + m.getContents());
//		logger.debug("---- Autocommit  " + autoCommit + ". Client id: " + proxy.getProcessId());
		byte[] response = null;
		
		if(opCode == OpcodeList.COMMIT || opCode == OpcodeList.ROLLBACK_SEND
				|| opCode == OpcodeList.COMMIT_AUTO	|| opCode == OpcodeList.CLOSE
				|| opCode == OpcodeList.LOGIN_SEND || opCode == OpcodeList.GET_DB_METADATA) {
			boolean commitByAutoCommitChange = false;
			if((!transactionReadOnly && opCode == OpcodeList.COMMIT_AUTO && (Boolean)m.getContents().equals(true)))
				commitByAutoCommitChange = true;
			if(opCode == OpcodeList.COMMIT || opCode == OpcodeList.ROLLBACK_SEND || commitByAutoCommitChange) {
				m = prepareFinishTransactionRequest(opCode);
				transactionReadOnly = true;
			}
			response = proxy.invokeOrdered(m.getBytes());
			if(opCode == OpcodeList.COMMIT || opCode == OpcodeList.ROLLBACK_SEND || commitByAutoCommitChange)
				clearOpsRes();
		} else {
			if(autoCommit) {
				if(opCode == OpcodeList.EXECUTE_UPDATE || opCode == OpcodeList.EXECUTE_BATCH) {
					response = proxy.invokeOrdered(m.getBytes());
				} else {
					response = proxy.invokeUnordered(m.getBytes());
				}
			} else {
				if(opCode == OpcodeList.EXECUTE_UPDATE || opCode == OpcodeList.EXECUTE_BATCH)
					transactionReadOnly = false;
				if(!transactionReadOnly)
					operations.add(m);
				int[] processes = new int[] {master};
				SteelDBListener steelListener = new SteelDBListener(m.getBytes(), new BFTComparator(), new BFTExtractor(), master);
				try {
					proxy.invokeAsynchronous(m.getBytes(), steelListener, processes, TOMMessageType.UNORDERED_REQUEST);
				} catch(Exception ex) {
					logger.error("The master replica is not reacheable", ex);
				}
				TOMMessage message = steelListener.getResponse(); 
//					logger.debug("Message:" + message);
				if(message != null) { 
					response = message.getContent();
				} else { // the master didn't reply on time. Will invoke a master change
					logger.info("client " + clientId + " invoking master change");
					if(operations.size() == 0)
						operations.add(m);
					Message replyMsg = invokeMasterChange();
					if(replyMsg == null) {
						return new Message(OpcodeList.MASTER_CHANGE_ERROR, null, false, master);
					} else {
						logger.info("master change executed for " + clientId + ", status: " + replyMsg.getOpcode());
						if(replyMsg.getContents() instanceof ResultSetData) {
							ResultSetData rsd = (ResultSetData) replyMsg.getContents();
							try {
								BFTRowSet bftrs = new BFTRowSet();
								bftrs.populate(rsd);
								return new Message(replyMsg.getOpcode(), bftrs, replyMsg.isUnordered(), replyMsg.getMaster(), replyMsg.getStatementOption());
							} catch (SQLException e) {
								logger.error("Error populating BFTRowSet", e);
							}
						} else
							return replyMsg;
					}
				}
			}
		}
		
		Message reply = Message.getMessage(response);
		if(reply != null) {
			master = reply.getMaster();
			logger.debug("Reply opcode: " + m.getOpcode() + ", content: "  + reply.getContents());
		} else {
			logger.info("reply is null. " + m.getClientId() + ", opt: " + opCode + ", contents: " + m.getContents());
		}
		
		Object replyContent = reply.getContents();
		if(replyContent instanceof ResultSetData) {
			ResultSetData rsd = (ResultSetData) replyContent;
			try {
				BFTRowSet bftrs = new BFTRowSet();
				bftrs.populate(rsd);
				reply = new Message(reply.getOpcode(), bftrs, reply.isUnordered(), reply.getMaster(), reply.getStatementOption());
			} catch (SQLException e) {
				logger.error("Error populating BFTRowSet", e);
			}
		}
		
		if(!transactionReadOnly) {
			byte[] replyBytes = TOMUtil.getBytes(String.valueOf(replyContent));
			byte[] replyHash = TOMUtil.computeHash(replyBytes);
			resHashes.add(replyHash);
		}
		return reply;
	}

	public void close() {
//		logger.debug("Closing connection for client " + proxy.getProcessId());
		proxy.close();
	}


	private Message invokeMasterChange() {
		MasterChangeRequest MCRequest = new MasterChangeRequest(resHashes, operations);
		Message MCMessage = new Message(OpcodeList.MASTER_CHANGE, MCRequest, false, master);
		try {
			MCMessage.setClientId(clientId);
		} catch(Exception e) {
			logger.error("Error setting the clientId");
		}
		byte[] MCResponse = proxy.invokeOrdered(MCMessage.getBytes());
		Message MCReply = Message.getMessage(MCResponse);
		if(MCReply.getOpcode() == OpcodeList.MASTER_CHANGE_OK) {
			Message replyMsg = (Message)MCReply.getContents();
			oldMaster = master;
			master = (Integer)MCReply.getMaster();
			clearOpsRes();
			logger.info("New master defined: " + master);
			return replyMsg;
		} else
			return null;
	}
	
	static class BFTComparator implements Comparator<byte[]> {
		@Override
		public int compare(byte[] o1, byte[] o2) {
			Message m1 = Message.getMessage(o1);
			Message m2 = Message.getMessage(o2);
			
			if(m1.getOpcode() != m2.getOpcode()) {
				System.out.println("opcodes not matching: " + m1.getOpcode() + "," + m2.getOpcode());
				return -1;
			}
			
			if(m1.getContents() instanceof byte[] && m2.getContents() instanceof byte[]) {
				boolean byteArrayEquals = Arrays.equals((byte[])m1.getContents(), (byte[])m2.getContents());
				if(byteArrayEquals)
					return 0;
				else {
					System.out.println("byte arrays not matching");
					return -1;
				}
			}

			if((m1.getContents() == null && m2.getContents() == null) ||
							m1.getContents().equals(m2.getContents()))
				return 0;
			else {
				System.out.println("contents not matching: " + m1.getOpcode() + "," + m1.getContents() + "," + m2.getContents());
				return -1;
			}
		}
	}

	static class BFTExtractor implements Extractor {
		@Override
		public TOMMessage extractResponse(TOMMessage[] replies, int sameContent, int lastReceived) {
			TOMMessage reply = replies[lastReceived];
			if(reply == null)
				System.out.println("MessageHandler.extractresponse(): reply at position " + lastReceived + " is null");
			return reply;

		}
	}
	
	/**
	 * Iterates over the array of replica ids and find f plus one non master replicas 
	 * @return f plus one non master replicas
	 */
	private int[] getFPlusOneOthers(int operationId, int[] processes) {
		int fPlusOne = proxy.getViewManager().getCurrentViewF() + 1;
		int[] target = new int[fPlusOne];
		int counter = 0;
		int index = operationId % processes.length;
		while(counter < fPlusOne) {
			if(processes[index] != master) {
				target[counter] = processes[index];
				counter++;
			}
			index = (index + 1) % processes.length;
		}
		return target;
	}
	
	/**
	 * Creates a request to commit or rollback a transaction.
	 * Adds as content the lists of commands executed during the transaction
	 * and the respective responses.
	 * @param opcode The code defining if the request is for commit or rollback.
	 * @return The commit request message
	 */
	private Message prepareFinishTransactionRequest(int opcode) {
		FinishTransactionRequest finishReq = new FinishTransactionRequest(resHashes, operations);
		Message message;
		if(opcode == OpcodeList.ROLLBACK_SEND) {
			boolean masterChanged = oldMaster != master;
			RollbackRequest req = new RollbackRequest(masterChanged, oldMaster, finishReq);
			message = new Message(opcode, req, false, master);
		} else // commit
			message = new Message(opcode, finishReq, false, master);
		try {
			message.setClientId(clientId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return message;
	}
	
	/**
	 * Clears the lists of requests and responses.
	 * These lists are cleaned after a commit or rollback of a transaction.
	 */
	private void clearOpsRes() {
		this.resHashes = new LinkedList<byte[]>();
		this.operations = new LinkedList<Message>();
	}

}
