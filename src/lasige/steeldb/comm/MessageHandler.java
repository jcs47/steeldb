package lasige.steeldb.comm;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import lasige.steeldb.client.SteelDBListener;
import lasige.steeldb.jdbc.BFTRowSet;
import lasige.steeldb.jdbc.ResultSetData;

import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.Extractor;
import bftsmart.tom.util.TOMUtil;
import bftsmart.tom.util.Storage;
import java.util.HashMap;
import java.util.logging.Level;

public class MessageHandler {

	private AsynchServiceProxy proxy;
        
        //I suspect this optimization is causing read/write dependencies in
        //non-masters, which prevents operations from being all executed
        //private boolean transactionReadOnly;
	private boolean transactionReadOnly = false;
	private LinkedList<byte[]> resHashes;
	private LinkedList<Message> operations;
	private static final String CONFIG_FOLDER = System.getProperty("divdb.folder", "config");
	private static final int FIRST_CLIENT_ID = Integer.valueOf(System.getProperty("divdb.firstclient", "1000"));
	private final int clientId;
        
        private long nextSequence;
        protected HashMap<Long,Long> lastCommitedTrans = null;

        public int getClientId() {
            return clientId;
        }
        
	private int master;
	private int oldMaster;
        
	//private Storage storeSize = new Storage(100000);
        //private Storage storeOps = new Storage(100000);

        private Logger logger = Logger.getLogger("steeldb_client");
	
	public MessageHandler(int clientId) {
		clientId = FIRST_CLIENT_ID + clientId;
                //proxy = new ServiceProxy(clientId, CONFIG_FOLDER, new BFTComparator(), new BFTExtractor()); // old code fo smart
                proxy = new AsynchServiceProxy(clientId, CONFIG_FOLDER, new BFTComparator(), new BFTExtractor());
                
                //I suspect this optimization is causing read/write dependencies in
                //non-masters, which prevents operations from being all executed
		//transactionReadOnly = true;
		this.resHashes = new LinkedList<byte[]>();
		this.operations = new LinkedList<Message>();
                this.lastCommitedTrans = new HashMap<>();
		this.clientId = clientId;
		master = 0;
                nextSequence = 0;
		logger.debug("Client " + clientId + "opened a connection. MessageHandler created");
	}
	
	public MessageHandler(int clientId, boolean replica) {
                //proxy = new ServiceProxy(clientId, CONFIG_FOLDER, new BFTComparator(), new BFTExtractor()); // old code fo smart
                proxy = new AsynchServiceProxy(clientId, CONFIG_FOLDER, new BFTComparator(), new BFTExtractor());
                
                //I suspect this optimization is causing read/write dependencies in
                //non-masters, which prevents operations from being all executed
		//transactionReadOnly = true;
                this.lastCommitedTrans = new HashMap<>();
		this.clientId = clientId;
		master = 0;
                nextSequence = 0;
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
            m.setOpSequence(nextSequence++);
            Long l = lastCommitedTrans.remove(m.getOpSequence() - 1);
            m.setLastCommitedTransId((l != null ? l : -1));
            int opCode = m.getOpcode();
            logger.debug("---- Client " + clientId + ", OpCode: " + m.getOpcode() + ", Msg: " + m.getContents());
            if (m.getContents() instanceof String) logger.debug("---- Invoking query: '" + ((String) m.getContents()) + "'");
//		logger.debug("---- Autocommit  " + autoCommit + ". Client id: " + proxy.getProcessId());
            byte[] response = null;

            if(opCode == OpcodeList.COMMIT || opCode == OpcodeList.ROLLBACK_SEND
                        || opCode == OpcodeList.COMMIT_AUTO || opCode == OpcodeList.CLOSE
                        || opCode == OpcodeList.LOGIN_SEND  || opCode == OpcodeList.GET_DB_METADATA) {
                boolean commitByAutoCommitChange = false;
                if((!transactionReadOnly && opCode == OpcodeList.COMMIT_AUTO && (Boolean)m.getContents().equals(true)))
                    commitByAutoCommitChange = true;
                if(opCode == OpcodeList.COMMIT || opCode == OpcodeList.ROLLBACK_SEND || commitByAutoCommitChange) {
                    m = prepareFinishTransactionRequest(opCode, m.getOpSequence(), m.getLastCommitedTransId());
                    //transactionReadOnly = true;

                }
                
                byte[] b = m.getBytes();
                response = proxy.invokeOrdered(b);
                /*if(opCode == OpcodeList.COMMIT) {

                    storeSize.store(b.length);
                    storeOps.store(operations.size());

                    if (storeSize.getCount() % 100 == 0) {
                        System.out.println("\n"+Thread.currentThread().getName() + "// Average size for " +  storeSize.getCount() + " commits = " + storeSize.getAverage(false) + " bytes ");
                        System.out.println(Thread.currentThread().getName() + "// Standard desviation for " + storeSize.getCount() + " commits = " + storeSize.getDP(false));
                        System.out.println(Thread.currentThread().getName() + "// Maximum size for " + storeSize.getCount() + " commits = " + storeSize.getMax(false) + " bytes ");
                        System.out.println(Thread.currentThread().getName() + "// Median for " + storeSize.getCount() + " commits = " + storeSize.getPercentile(0.5) + " bytes ");
                        System.out.println(Thread.currentThread().getName() + "// 90th for " + storeSize.getCount() + " commits = " + storeSize.getPercentile(0.9) + " bytes ");
                        System.out.println(Thread.currentThread().getName() + "// 95th for " + storeSize.getCount() + " commits = " + storeSize.getPercentile(0.95) + " bytes ");
                        System.out.println(Thread.currentThread().getName() + "// 99th for " + storeSize.getCount() + " commits = " + storeSize.getPercentile(0.99) + " bytes ");
                    }

                    if (storeOps.getCount() % 100 == 0) {
                        System.out.println("\n"+Thread.currentThread().getName() + "// Average ops for " +  storeOps.getCount() + " commits = " + storeOps.getAverage(false) + " ops ");
                        System.out.println(Thread.currentThread().getName() + "// Standard desviation for " + storeOps.getCount() + " commits = " + storeOps.getDP(false));
                        System.out.println(Thread.currentThread().getName() + "// Maximum ops for " + storeOps.getCount() + " commits = " + storeOps.getMax(false) + " ops ");
                        System.out.println(Thread.currentThread().getName() + "// Median for " + storeOps.getCount() + " commits = " + storeOps.getPercentile(0.5) + " ops ");
                        System.out.println(Thread.currentThread().getName() + "// 90th for " + storeOps.getCount() + " commits = " + storeOps.getPercentile(0.9) + " ops ");
                        System.out.println(Thread.currentThread().getName() + "// 95th for " + storeOps.getCount() + " commits = " + storeOps.getPercentile(0.95) + " ops ");
                        System.out.println(Thread.currentThread().getName() + "// 99th for " + storeOps.getCount() + " commits = " + storeOps.getPercentile(0.99) + " ops ");
                    }
                }*/

                if(opCode == OpcodeList.COMMIT || opCode == OpcodeList.ROLLBACK_SEND || commitByAutoCommitChange)
                    clearOpsRes();
            } else {
                if(autoCommit) {

                    //I suspect this optimization is causing read/write dependencies in
                    //non-masters, which prevents operations from being all executed
                    //if(opCode == OpcodeList.EXECUTE_UPDATE || opCode == OpcodeList.EXECUTE_BATCH) {
                            response = proxy.invokeOrdered(m.getBytes());
                    //} else {
                    //	response = proxy.invokeUnordered(m.getBytes());
                    //}
                } else {
                    //I suspect this optimization is causing read/write dependencies in
                    //non-masters, which prevents operations from being all executed
                    //if(opCode == OpcodeList.EXECUTE_UPDATE || opCode == OpcodeList.EXECUTE_BATCH)
                        transactionReadOnly = false;
                    if(transactionReadOnly) {
                        response = proxy.invokeUnordered(m.getBytes());
                    } else {
                        
                        if (proxy.getViewManager().getCurrentViewN() > 1)
                            operations.add(m);
                        //int[] processes = new int[] {master};
                        int[] processes = proxy.getViewManager().getCurrentViewProcesses();
                        SteelDBListener steelListener = new SteelDBListener(clientId, m.getBytes(), new BFTComparator(), new BFTExtractor(), master);                        
                        try {
                            //proxy.invokeAsynchronous(m.getBytes(), steelListener, processes, TOMMessageType.UNORDERED_REQUEST); // old code of smart

                            proxy.invokeAsynchRequest(m.getBytes(), processes, steelListener, TOMMessageType.UNORDERED_REQUEST);
                        } catch(Exception ex) {
                            logger.error("The master replica is not reacheable", ex);
                        }
                        TOMMessage message = steelListener.getResponse(); 
//						logger.debug("Message:" + message);
                        if(message != null) { 

                            proxy.cleanAsynchRequest(message.getSequence());
                            response = message.getContent();
                        } else { // the master didn't reply on time. Will invoke a master change
                            logger.info("client " + clientId + " invoking master change");
                            if(proxy.getViewManager().getCurrentViewN() > 1 && operations.size() == 0)
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
                                        Message ret = new Message(replyMsg.getOpcode(), bftrs, replyMsg.isUnordered(), replyMsg.getMaster());
                                        ret.setAutoGeneratedKeys(replyMsg.getAutoGeneratedKeys());
                                        ret.setResultSetType(replyMsg.getResultSetType());
                                        ret.setResultSetConcurrency(replyMsg.getResultSetConcurrency());
                                        return ret;
                                    } catch (SQLException e) {
                                            logger.error("Error populating BFTRowSet", e);
                                    }
                                } else
                                    return replyMsg;
                            }
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

            //if (reply.getOpcode() == OpcodeList.EXECUTE_OK ||  reply.getOpcode() == OpcodeList.EXECUTE_QUERY_OK || 
            //        reply.getOpcode() == OpcodeList.EXECUTE_UPDATE_OK || reply.getOpcode() == OpcodeList.EXECUTE_BATCH_OK)
            if (reply.getLastCommitedTransId() != -1)
                lastCommitedTrans.put(m.getOpSequence(), reply.getLastCommitedTransId());
            
            Object replyContent = reply.getContents();
            if(replyContent instanceof ResultSetData) {
                    ResultSetData rsd = (ResultSetData) replyContent;
                    try {
                            BFTRowSet bftrs = new BFTRowSet();
                            bftrs.populate(rsd);
                            Message tmp = new Message(reply.getOpcode(), bftrs, reply.isUnordered(), reply.getMaster());                                
                            tmp.setAutoGeneratedKeys(reply.getAutoGeneratedKeys());
                            tmp.setResultSetType(reply.getResultSetType());
                            tmp.setResultSetConcurrency(reply.getResultSetConcurrency());
                            reply = tmp;
                    } catch (SQLException e) {
                            logger.error("Error populating BFTRowSet", e);
                    }
            }

            //I suspect this optimization is causing read/write dependencies in
            //non-masters, which prevents operations from being all executed
            //if(!transactionReadOnly) {
            if(proxy.getViewManager().getCurrentViewN() > 1 && !transactionReadOnly && !autoCommit && opCode != OpcodeList.COMMIT && opCode != OpcodeList.ROLLBACK_SEND
                            && opCode != OpcodeList.COMMIT_AUTO	&& opCode != OpcodeList.CLOSE
                            && opCode != OpcodeList.LOGIN_SEND && opCode != OpcodeList.GET_DB_METADATA) {
                    byte[] replyBytes = TOMUtil.getBytes(replyContent);
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

			if (m1.getContents() != null && m2.getContents() != null &&
							m1.getContents().equals(m2.getContents()))
				return 0;
                        else if (m1.getContents() == null && m2.getContents() == null)
                                return 0;
                        else if (m1.getContents() instanceof Exception && m2.getContents() instanceof Exception) {
                            
                            Exception ex1 = (Exception) m1.getContents();
                            Exception ex2 = (Exception) m2.getContents();
                            
                            return (ex1.getMessage().equals(ex2.getMessage()) ? 0 : -1);
                        }
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
	private Message prepareFinishTransactionRequest(int opcode, long sequence, long transId) {

                FinishTransactionRequest finishReq = new FinishTransactionRequest(resHashes, (operations.peekFirst() != null ? operations.peekFirst().getOpSequence() : -1));

                //FinishTransactionRequest finishReq = new FinishTransactionRequest(null, (operations.peekFirst() != null ? operations.peekFirst().getOpSequence() : -1));
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
                message.setOpSequence(sequence);
                message.setLastCommitedTransId(transId);
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
        
        public boolean hasOps() {
            return operations.size() == resHashes.size() && operations.size() > 0;
        }
        
        public int getMaster() {
            return master;
        }

        public int getOldMaster() {
            return oldMaster;
        }
}
