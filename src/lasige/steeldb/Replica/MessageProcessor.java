package lasige.steeldb.Replica;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import bftsmart.reconfiguration.views.View;
import bftsmart.tom.util.TOMUtil;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import lasige.steeldb.Replica.normalizaton.FirebirdNormalizer;
import lasige.steeldb.Replica.normalizaton.NoNormalizer;
import lasige.steeldb.Replica.normalizaton.Normalizer;
import lasige.steeldb.comm.FinishTransactionRequest;
import lasige.steeldb.comm.LoginRequest;
import lasige.steeldb.comm.MasterChangeRequest;
import lasige.steeldb.comm.Message;
import lasige.steeldb.comm.OpcodeList;
import lasige.steeldb.comm.RollbackRequest;
import lasige.steeldb.jdbc.BFTDatabaseMetaData;
import lasige.steeldb.jdbc.BFTRowSet;
import lasige.steeldb.jdbc.ResultSetData;
import lasige.steeldb.statemanagement.DBConnectionParams;

public class MessageProcessor {
	private int replicaId;
	private SessionManager sm;

        public SessionManager getSessionManager() {
            return sm;
        }
        
	private int master;
	private Normalizer norm;
	private View currentView;
	private long lastMasterChange;
	private boolean installingState;
        private ConcurrentHashMap<Integer, LinkedList<byte[]>> results;
        private ConcurrentHashMap<Integer, SpeculativeThread> threads;
        private long lastCommitedTransId = 0;
                
    private static Logger logger = Logger.getLogger("steeldb_processor");

    public MessageProcessor(int id, String driver, String url) {
		this.replicaId = id;
		this.norm = getNormalizer(driver);
		this.sm = new SessionManager(url);
		this.master = 0;
		installingState = false;
                results = new ConcurrentHashMap<>();
                threads = new ConcurrentHashMap<>();
	}
	
        protected void processOperation(Message m, int clientId) {
            
                ConnManager connManager = sm.getConnManager(clientId);
                
                /*if (m.getOpcode() != OpcodeList.COMMIT && m.getOpcode() != OpcodeList.ROLLBACK_SEND)
                    connManager.enqueueOperation(m);
                
                if (m.getLastCommitedTransId() != -1) {
                    
                    Message[] op = connManager.pollMessages(m.getOpSequence() - 1 , 1);
                    sm.waitforCommit(m.getLastCommitedTransId(), clientId);
                    byte[] replyHash = executeOperation(op[0], clientId);
                    results.get(clientId).add(replyHash);
                }*/
                
                SpeculativeThread sp = threads.get(clientId);
                if (sp == null) {
                    sp = new SpeculativeThread(clientId, m.getOpSequence());
                    sp.start();
                    threads.put(clientId, sp);
                }
                        
                connManager.enqueueOperation(m);
        }
        
        protected byte[] executeOperation(Message m, int clientId) {
            
                Message result = null;
                switch(m.getOpcode()) {
                    case OpcodeList.EXECUTE:
                            result = processExecute(m,clientId);
                            break;
                    case OpcodeList.EXECUTE_BATCH:
                            result = processExecuteBatch(m, clientId);
                            break;
                    case OpcodeList.EXECUTE_QUERY:
                            result = processExecuteQuery(m, clientId);
                            break;
                    case OpcodeList.EXECUTE_UPDATE:
                            result = processExecuteUpdate(m, clientId);
                            break;
                    default:
                            result = new Message(OpcodeList.COMMIT_ERROR, new SQLException("Unknown op: " + m.getOpcode()), false, master);
                    }
                Object replyContent = result.getContents();
                byte[] replyBytes = null;
                replyBytes = TOMUtil.getBytes(replyContent);
                return TOMUtil.computeHash(replyBytes);            
        }
        
        public void finishedTransaction(int clientId) {

            this.lastCommitedTransId++;
            logger.debug("---- Client " + clientId + " finished commit #" + lastCommitedTransId + ", notifying spec threads");

            Collection<SpeculativeThread> specs = threads.values();
            
            for (SpeculativeThread s : specs) {
            
                synchronized(s) {
                    s.notify();
                }
            }
     }
        
        public Long getLastCommitedTrans() {
            return lastCommitedTransId;
        }
         
	protected Message processExecute(Message m, int clientId) {
		ConnManager connManager = sm.getConnManager(clientId);
		String sql = (String)m.getContents();
		Statement s;
		Message reply = null;
		if(connManager.isAborted()) {
			return new Message(OpcodeList.ABORTED, new SQLException("Connection aborted"), false, master);
		}
		int content = 0;
		//normalize statement
		String normSql = norm.normalize(sql);
		try {
			s = connManager.createStatement();
                        logger.debug("!---- Client: " + clientId + ", EXECUTE: " + sql);
			if(s.execute(normSql))
				content = 1; // 1 = true, 0 = false
			reply = new Message(OpcodeList.EXECUTE_OK, content,false, master);
			logger.debug(clientId + "--- RESULT EXECUTE " + content);
		} catch (SQLException sqle) {
			logger.error("---- Client: " + clientId + ", EXECUTE ERROR", sqle);
			//sqle.printStackTrace();
			reply = new Message(OpcodeList.EXECUTE_ERROR, sqle,false, master);
		}
                logger.debug("!---- Client: " + clientId + ", EXECUTE REPLY: " + reply.toString());
		if (this.master == this.replicaId /*&& reply.getOpcode() == OpcodeList.EXECUTE_OK*/)
                    reply.setLastCommitedTransId(getLastCommitedTrans());
                return reply;


	}

	protected Message processExecuteBatch(Message m, int clientId) {
		Message reply = null;
		ConnManager connManager = sm.getConnManager(clientId);
		if(connManager.isAborted()) {
			return new Message(OpcodeList.ABORTED, new SQLException("Connection aborted"), false, master);
		}
		@SuppressWarnings("unchecked")
		LinkedList<String> batch = (LinkedList<String>) m.getContents();
		try {
			Statement s = connManager.createStatement();
			for(String sql: batch) {
				s.addBatch(sql);
                                logger.debug("!---- Client: " + clientId + ", BATCH: " + sql);
			}
			int[] resultArray = s.executeBatch();
			ArrayList<Integer> result = new ArrayList<Integer>(resultArray.length);
			for(int i = 0; i < resultArray.length; i++)
				result.add(resultArray[i]);
			reply = new Message(OpcodeList.EXECUTE_BATCH_OK, result, false, master);
		} catch (SQLException sqle) {
			logger.error("---- Client: " + clientId + ", BATCH ERROR", sqle);
			reply = new Message(OpcodeList.EXECUTE_BATCH_ERROR, sqle, false, master);
		}
                logger.debug("!---- Client: " + clientId + ", BATCH REPLY: " + reply.toString());
                if (this.master == this.replicaId /*&& reply.getOpcode() == OpcodeList.EXECUTE_BATCH_OK*/)
                    reply.setLastCommitedTransId(getLastCommitedTrans());
                return reply;
	}

	protected Message processExecuteQuery(Message m, int clientId) {
		Message reply = null;
		ConnManager connManager = sm.getConnManager(clientId);
		if(connManager.isAborted()) {
			return new Message(OpcodeList.ABORTED, new SQLException("Connection aborted"), false, master);
		}
		String sql = (String)m.getContents();
		Statement s;
                ResultSetData rsd;
                ResultSet rs;
		try {
                    
                    //I am not sure if a query needs to validate 'Statement.RETURN_GENERATED_KEYS',
                    // so I leave here code that might be needed eventually
                    
                   /* if(m.getAutoGeneratedKeys() == Statement.RETURN_GENERATED_KEYS) {
                        s = connManager.createStatement();
                        logger.debug("!---- Client: " + clientId + ", QUERY: " + sql);
                        int result = ((PreparedStatement) s).executeUpdate();
                        BFTRowSet rowset = new BFTRowSet();
                        rowset.populate(s.getGeneratedKeys());
                        reply = new Message(OpcodeList.EXECUTE_QUERY_OK, result, true, master, Statement.RETURN_GENERATED_KEYS);
                         reply.setRowset(rowset);
                    }
                    else*/
                    if ((m.getResultSetType() != ResultSet.TYPE_FORWARD_ONLY) || (m.getResultSetConcurrency() != ResultSet.CONCUR_READ_ONLY)) {

                        // Disabled these options because of performance issues
                        s = connManager.createStatement(m.getResultSetType(), m.getResultSetConcurrency());
                        //s = connManager.createStatement();
                        logger.debug("!---- Client: " + clientId + ", QUERY: " + sql);
                        rs = s.executeQuery(sql);
                        rsd = new ResultSetData(rs);
                        //logger.debug("---- Client: " + clientId + ", QUERY RESULT: " + rsd);
                        logger.debug("---- Client: " + clientId + ", METADATA HASH: " + Arrays.toString(TOMUtil.computeHash(TOMUtil.getBytes(rsd.getMetadata()))) + ", rsd.getRows hash: " + Arrays.toString(TOMUtil.computeHash(TOMUtil.getBytes(rsd.getRows()))));
                        reply = new Message(OpcodeList.EXECUTE_QUERY_OK, rsd, true, master, m.getResultSetType(), m.getResultSetConcurrency());
                        //reply.setRowset(rowset);
                    } else {
                        s = connManager.createStatement();
                        logger.debug("!---- Client: " + clientId + ", QUERY: " + sql);
                        rs = s.executeQuery(sql);
                        rsd = new ResultSetData(rs);
                        //logger.debug("---- Client: " + clientId + ", QUERY RESULT: " + rsd);
                        logger.debug("---- Client: " + clientId + ", METADATA HASH: " + Arrays.toString(TOMUtil.computeHash(TOMUtil.getBytes(rsd.getMetadata()))) + ", rsd.getRows hash: " + Arrays.toString(TOMUtil.computeHash(TOMUtil.getBytes(rsd.getRows()))));
                        reply = new Message(OpcodeList.EXECUTE_QUERY_OK, rsd, true, master);
                    }
                } catch (SQLException sqle) {
			logger.error("---- Client: " + clientId + ", QUERY ERROR", sqle);
			reply = new Message(OpcodeList.EXECUTE_QUERY_ERROR, sqle, true, master);
		}
                logger.debug("!---- Client: " + clientId + ", QUERY REPLY: " + reply.toString());
		if (this.master == this.replicaId /*&& reply.getOpcode() == OpcodeList.EXECUTE_QUERY_OK*/)
                    reply.setLastCommitedTransId(getLastCommitedTrans());
                return reply;

	}

	protected Message processExecuteUpdate(Message m, int clientId) {
		ConnManager connManager = sm.getConnManager(clientId);
		
		if(connManager.isAborted()) {
			return new Message(OpcodeList.ABORTED, new SQLException("Connection aborted"), false, master);
		}

		Message reply = null;
		String sql = (String)m.getContents();
		
		Statement s;
		try {			
			int result = -1;
                        
                        //I am not sure if an update needs to validate 'ResultSet.TYPE_FORWARD_ONLY' or 'ResultSet.CONCUR_READ_ONLY',
                        // so I leave here code that might be needed eventually

                        /*if ((m.getResultSetType() != ResultSet.TYPE_FORWARD_ONLY) || (m.getResultSetConcurrency() != ResultSet.CONCUR_READ_ONLY)) {

                            s = connManager.prepareStatement(sql, m.getResultSetType(), m.getResultSetConcurrency());
                            logger.debug("!---- Client: " + clientId + ", UPDATE: " + sql);
                            result = ((PreparedStatement) s).executeUpdate();
                            BFTRowSet rowset = new BFTRowSet();
                            rowset.populate(s.getGeneratedKeys());
                            reply = new Message(OpcodeList.EXECUTE_UPDATE_OK, result, false, master, m.getResultSetType(), m.getResultSetConcurrency());
                            reply.setRowset(rowset);
                        
                        } else */
                        
			if(m.getAutoGeneratedKeys() == Statement.RETURN_GENERATED_KEYS) {                            
                            s = connManager.createStatement();
                            logger.debug("!---- Client: " + clientId + ", UPDATE: " + sql);
                            result = s.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                            BFTRowSet rowset = new BFTRowSet();
                            rowset.populate(s.getGeneratedKeys());
                            reply = new Message(OpcodeList.EXECUTE_UPDATE_OK, result, false, master, Statement.RETURN_GENERATED_KEYS);
                            reply.setRowset(rowset);
                                
			} else {
                            s = connManager.createStatement();
                            logger.debug("!---- Client: " + clientId + ", UPDATE: " + sql);
                            result = s.executeUpdate(sql);
                            reply = new Message(OpcodeList.EXECUTE_UPDATE_OK, result, false, master);
			}
		} catch (SQLException sqle) {
			logger.error("---- Client: " + clientId + ", UPDATE ERROR", sqle);
			reply = new Message(OpcodeList.EXECUTE_UPDATE_ERROR, sqle, false, master);
		}
                logger.debug("!---- Client: " + clientId + ", UPDATE REPLY: " + reply.toString());
		if (this.master == this.replicaId /*&& reply.getOpcode() == OpcodeList.EXECUTE_UPDATE_OK*/)
                    reply.setLastCommitedTransId(getLastCommitedTrans());
                return reply;
	}

	protected Message processLogin(Message m, int clientId) {
		logger.debug("---- Client: " + clientId + ", PROCESSING LOGIN");
		LoginRequest lr = (LoginRequest)m.getContents();
		String database = lr.getDatabase(replicaId);
		String user = lr.getUser(replicaId);
		String password = lr.getPassword(replicaId);
		Message reply;
		if(sm.connect(clientId, database, user, password)) {
                        results.put(clientId, new LinkedList<>());
                        reply = new Message(OpcodeList.LOGIN_OK, null, false, master);
		} else {
			reply = new Message(OpcodeList.LOGIN_ERROR, null, false, master);
		}
		return reply;
	}
	
	protected Message getDBMetadata(int clientId) {
		Message reply = null;
		ConnManager connManager = sm.getConnManager(clientId);
		
		DatabaseMetaData dbm;
		try {
			dbm = connManager.getMetaData();
			BFTDatabaseMetaData bftDBM = new BFTDatabaseMetaData(dbm);
			if(dbm != null)
				reply = new Message(OpcodeList.GET_DB_METADATA_OK, bftDBM, false, master);
			else
				reply = new Message(OpcodeList.GET_DB_METADATA_ERROR, bftDBM, false, master);

			return reply;
		} catch (SQLException sqle) {
			logger.error("---- Client: " + clientId +  ", getDBMetadata() error: ", sqle);
			sqle.printStackTrace();
			return new Message(OpcodeList.GET_DB_METADATA_ERROR, sqle, false, master);
		}
	}

	protected Message processCommit(Message m, int clientId) {
		FinishTransactionRequest finishReq = (FinishTransactionRequest) m.getContents();
		LinkedList<byte[]> resHashes = finishReq.getResHashes();
		//LinkedList<Message> operations = finishReq.getOperations();
		Message reply = null;
		if(resHashes != null && resHashes.size() > 0) {
			reply = processFinishTransaction(m, clientId, OpcodeList.COMMIT);
		}
		if(reply == null || reply.getOpcode() == OpcodeList.COMMIT_OK){
			try {
                                ConnManager connManager = sm.getConnManager(clientId);
                                finishedTransaction(clientId); //increment transaction ID
                                connManager.commit();
				if(reply == null)
					reply = new Message(OpcodeList.COMMIT_OK, null, false, master);
			} catch (SQLException sqle) {
                                logger.error("---- Client: " + clientId +  ", processCommit() error: ", sqle);
				reply = new Message(OpcodeList.COMMIT_ERROR, sqle, true, master);
			}
		}
		return reply;
	}
	
	/**
	 * This method is used for commit and rollback operations.
	 * During commit, queued operations in all replicas must be confronted
	 * to the operations executed on the master to garantee that the
	 * master is not byzantine. During rollback the same happens. In cases
	 * of error messages when the transaction has to be aborted, the error
	 * has to be reproduced in the other replicas also to validate that
	 * the master is correct.
	 * @param m The commit or rollback request.
	 * @param clientId The client id used to get the queue of operations.
	 * @param opcode Commit or rollback.
	 * @return Success or error on commit or rollback.
	 */
	private Message processFinishTransaction(Message m, int clientId, int opcode) {
                logger.debug("---- Client: " + clientId + ", PROCESSING TRANSACTION");
		ConnManager connManager = sm.getConnManager(clientId);
		Message reply = null;
		int opcodeError = OpcodeList.COMMIT_ERROR;
		int opcodeOK = OpcodeList.COMMIT_OK;
		if(opcode == OpcodeList.ROLLBACK_SEND) {
			opcodeError = OpcodeList.ROLLBACK_ERROR;
			opcodeOK = OpcodeList.ROLLBACK_OK;
		}
		if(this.master != this.replicaId || installingState) {
			FinishTransactionRequest finishReq = (FinishTransactionRequest) m.getContents();
			LinkedList<byte[]> resHashes = finishReq.getResHashes();

			connManager.setCommitingTransaction(true);

                        /*Message[] operations = connManager.pollMessages(finishReq.getFirstOpSequence(), (int) (m.getOpSequence() - finishReq.getFirstOpSequence()));

			for(Message msg: operations) {

                                byte[] replyHash = executeOperation(msg, clientId);
				results.add(replyHash);
			}*/

                        SpeculativeThread sp = threads.get(clientId);
                        if (sp != null) {
                            logger.debug("---- Client: " + clientId + ", inserting commit/rollback #" + m.getOpSequence());
                            connManager.enqueueOperation(m); // force last op to execute
                            logger.debug("---- Client: " + clientId + ", waiting for execution of op #" + m.getOpSequence());
                            sp.waitForOps(m.getOpSequence()); // wait for all ops to complete
                            logger.debug("---- Client: " + clientId + ", resuming spec thread ");
                            sp.resumeSpec();
                        }

                        logger.debug("---- Client: " + clientId + ", comparing hashes");
                        
			if(!compareHashes(resHashes, results.get(clientId))){
				logger.debug("---- Client: " + clientId + ", result hashes don't match. Results.size(): " + results.size() + ", resHashes.size(): " + resHashes.size());
				reply = new Message(opcodeError, new SQLException("Results hashes don't match"), true, master);
                                //reply = new Message(opcodeOK, null, false, master);
			} else {
				logger.debug("---- Client: " + clientId + ", result hashes match. Results.size(): " + results.size());
				reply = new Message(opcodeOK, null, false, master);
			}
                        results.get(clientId).clear();
		} else {
			reply = new Message(opcodeOK, null, false, master);
		}
		logger.debug("---- Client: " + clientId + ", PROCESSED TRANSACTION");
		return reply;
	}
	
	/**
	 * Process a rollback request from the client. If it was triggered by
	 * a master changed ocurred before, the transactions in the old master
	 * must be rolled back. The transactions in the remaining replicas must
	 * only to be reset. In cases where this rollback wasn't triggered due
	 * to master changes occured before, it is called the normal behavior,
	 * where processFinishTransaction compares operations and responses hashes.
	 * @param m the RollBackRequest message
	 * @param clientId the client who sent the message
	 * @return ROLLBACK_OK or ROLLBACK_ERROR if there was any exception.
	 */
	protected Message processRollback(Message m, int clientId) {
		Message reply = null;
		RollbackRequest rollReq = (RollbackRequest)m.getContents();
		logger.debug("---- Client: " + clientId + ", ROLLBACKING (" + rollReq.getOldMaster() + " " + replicaId + ")");
		if(rollReq.isMasterChanged()) {
			ConnManager connManager = sm.getConnManager(clientId);
			if(rollReq.getOldMaster() == replicaId) {
				try {
					logger.debug("---- Client: " + clientId + ", rollbacking transaction in the old master");
                                        //finishedTransaction(clientId); //increment transaction ID
					connManager.rollback();
                                        logger.debug("---- Client: " + clientId + ", rollbacked transaction in the old master");
				} catch (SQLException sqle) {
					logger.error("---- Client: " + clientId + ", ROLLBACK ERROR", sqle);
//					sqle.printStackTrace();
					reply = new Message(OpcodeList.ROLLBACK_ERROR, sqle, false, master);
					return reply;
				}
			} else {
				logger.debug("---- Client: " + clientId + ", reseting transaction properties");
				connManager.reset();
			}
			reply = new Message(OpcodeList.ROLLBACK_OK, null, false, master);
		} else {
                    
                        //Is it really necessary to execute the updates if the intention is to perform a rollback??
			FinishTransactionRequest finishReq = rollReq.getFinishReq();
			if(finishReq.getResHashes() != null && finishReq.getResHashes().size() > 0) {
                                long opSeq = m.getOpSequence();
                                long commitId = m.getLastCommitedTransId();
				m = new Message(OpcodeList.ROLLBACK_SEND, rollReq.getFinishReq(), false, master);
                            try {
                                m.setClientId(clientId);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                                m.setLastCommitedTransId(commitId);
                                m.setOpSequence(opSeq);
				reply = processFinishTransaction(m, clientId, OpcodeList.ROLLBACK_SEND);
			}
			if(reply == null || reply.getOpcode() == OpcodeList.ROLLBACK_OK) {
				try {
                                        ConnManager connManager = sm.getConnManager(clientId);
                                        logger.debug("---- Client: " + clientId + ", rollbacking transaction");
                                        //finishedTransaction(clientId); //increment transaction ID
                                        connManager.rollback();
                                        logger.debug("---- Client: " + clientId + ", rollbacked transaction");
					if(reply == null)
						reply = new Message(OpcodeList.ROLLBACK_OK, null, false, master);
				} catch (SQLException sqle) {
					logger.error("---- Client: " + clientId + ", ROLLBACK ERROR", sqle);
//					sqle.printStackTrace();
					reply = new Message(OpcodeList.ROLLBACK_ERROR, sqle, true, master);
				}
			}
		}
		return reply;
	}

	protected Message processAutoCommit(Message m, int clientId) {
		Message reply = null;
		boolean autoCommit = (Boolean)m.getContents();
		ConnManager connManager = sm.getConnManager(clientId);
		logger.debug("---- Client: " + clientId + ", SET AUTO-COMMIT: " + autoCommit);
		try {
			connManager.setAutoCommit(autoCommit);
			reply = new Message(OpcodeList.COMMIT_AUTO_OK,null, true, master);
		} catch (SQLException sqle) {
			logger.error("---- Client: " + clientId + ", SET AUTO-COMMIT ERROR", sqle);
//			sqle.printStackTrace();
			reply = new Message(OpcodeList.COMMIT_AUTO_ERROR,null, true, master);
		}
		return reply;
	}

	/**
	 * Process a request from the client to change the master.
	 * Request to the session manager to abort all open read-write transactions.
	 * Change the current master to be the next one in a round robin fashion. 
	 * 
	 * @param m the message requesting a master change. It is useful to record the client
	 * id and prevent the same client to request multiple master changes 
	 * @return the message confirming that the transactions were aborted, the master changed
	 * and the id of the new master
	 */
	public Message processMasterChange(Message m) {
		Message reply;
		long secondsSinceLastMC = System.currentTimeMillis() - lastMasterChange;
		lastMasterChange = System.currentTimeMillis();
		int oldMaster = m.getMaster();
		master = (oldMaster + 1) % currentView.getN();
		logger.info("---- Client: " + m.getClientId() + ", MASTER CHANGE, OLD: " + oldMaster + ", NEW: " + master);
		Message replyToLastRequest = executePreviousOps((MasterChangeRequest)m.getContents(), m.getClientId());
		if(replyToLastRequest != null)
			reply = new Message(OpcodeList.MASTER_CHANGE_OK, replyToLastRequest, false, master);
		else {
			reply = new Message(OpcodeList.MASTER_CHANGE_ERROR, null, false, master);
                        logger.error("---- Client: " + m.getClientId() + ", MASTER CHANGE ERROR");
		}
		return reply;
	}
	
	/**
	 * Execute the pending operations in open read write transactions in the new master.
	 * @return true if all operations were executed with success
	 */
	private Message executePreviousOps(MasterChangeRequest MCReq, int clientId) {
		LinkedList<Message> operations = MCReq.getOperations();
		LinkedList<byte[]> resHashes = MCReq.getResHashes();
		Message reply = null;
		if(resHashes == null && operations.size() > 1) {
			logger.error("---- Client: " + clientId + ", resHashes is null but operations.size is greater than 1");
			return null;
		}
		else if(resHashes.size()+1 != operations.size()) {
			logger.error("---- Client: " + clientId + ", previous ops on master change. Number of operations differs from number of replies.");
			return null;
		} else {
			logger.info("---- Client: " + clientId + ", resHashes.size: " + resHashes.size() + ", operations.size: " + operations.size());
		}
			
		for(int i = 0; i < operations.size(); i++) {
			Message msg = operations.get(i);
			switch(msg.getOpcode()) {
			case OpcodeList.EXECUTE:
				reply = processExecute(msg,clientId);
				break;
			case OpcodeList.EXECUTE_BATCH:
				reply = processExecuteBatch(msg, clientId);
				break;
			case OpcodeList.EXECUTE_QUERY:
				reply = processExecuteQuery(msg, clientId);
				break;
			case OpcodeList.EXECUTE_UPDATE:
				reply = processExecuteUpdate(msg, clientId);
				break;
			default:
				reply = null;
			}
			Object replyContent = reply.getContents();
			byte[] replyBytes = TOMUtil.getBytes(String.valueOf(replyContent));
			byte[] replyHash = TOMUtil.computeHash(replyBytes);
			if(i < resHashes.size()) {
				if(!Arrays.equals(replyHash, resHashes.get(i))) {
					return null;
				}
			} else {
				logger.info("---- Client: " + clientId + ", executed operation after master change: " + msg.getContents());
			}
		}
		return reply;
	}
	
	public Message processSetFetchSize(Message m, int clientId) {
		Message reply = new Message(OpcodeList.UNKNOWN_OP, null, true, master);
		return reply;
	}
	public Message processSetMaxRows(Message m, int clientId) {
		Message reply = new Message(OpcodeList.UNKNOWN_OP, null, true, master);
		return reply;
	}

	protected Message processClose(Message m, int clientId) {
		logger.debug("---- Client: " + clientId + ", CLOSE");
		Message reply;
		try {
                        results.get(clientId).clear();
                        
                        SpeculativeThread sp = threads.remove(clientId);
                        if (sp != null) sp.shutDown();
                        
			sm.close(clientId);
			reply = new Message(OpcodeList.CLOSE_OK, null, true, master);
		} catch (SQLException sqle) {
			logger.error("---- Client: " + clientId + ", CLOSE ERROR", sqle);
//			sqle.printStackTrace();
			reply = new Message(OpcodeList.CLOSE_ERROR, null, true, master);
		}
		return reply;
	}

	private Normalizer getNormalizer(String driver) {
		if(driver.equalsIgnoreCase("org.firebirdsql.jdbc.FBDriver"))
			return new FirebirdNormalizer();
		else
			return new NoNormalizer();
	}
        
	/**
	 * Gets from the session manager the open read-write transactions.
	 * @return A map with the client id and the list of operations executed
	 * in the transaction until the moment of the checkpoint.
	 */
	protected List<DBConnectionParams> getConnections() {
		return sm.getConnections();
	}
	
	/**
	 * This method is executed in the replica that requested the state transfer.
	 * It is called after the replica installed the dump and before execute the log
	 * of operations. It opens the connections to the database, creates the
	 * transactions and execute the operations that ran in the seeder before the
	 * checkpoint was taken. After running all the operations, the replica will
	 * have the state necessary to process the log and close the open transactions.
	 * @param transactions The map with client id and operations of the read-write
	 * transactions.
	 * @param connParams The parameters to log into the database and execute the
	 * operations. 
	 */
	protected void restoreOpenConnections(List<DBConnectionParams> connections, String database) {
		for(DBConnectionParams connection : connections) {
			logger.debug("---- RESTORING CONNECTION FOR CLIENT " + connection.getClientId());
			sm.connect(connection.getClientId(), database, connection.getUser(), connection.getPassword());
		}
		logger.debug("---- RESTORED CONNECTIONS");
	}
	
	
	
	/**
	 * Iterates through every item in the pending operations list and
	 * compute the hash value for the operation.
	 * @return a list with the hashes for each operation
	 */
	private LinkedList<byte[]> pendingOpsHashes(Queue<Message> queue) {
		LinkedList<byte[]> queueHashes = new LinkedList<byte[]>();
		for(Message m : queue) {
			byte[] hash = TOMUtil.computeHash(TOMUtil.getBytes(m.getContents()));
			queueHashes.add(hash);
		}
		return queueHashes;
	}

	private boolean compareHashes(LinkedList<byte[]> a, LinkedList<byte[]> b) {
		if(a == null)
			if(b ==  null)
				return true;
			else
				return false;
		if(a != null)
			if(b == null)
				return false;
			else {
				if(a.size() != b.size())
					return false;
				else {
					for(int i = 0; i < a.size(); i++) {
//						logger.debug("compareHashes " + i + ". a:" + Arrays.toString(a.get(i)) + ", b: " + Arrays.toString(b.get(i)));
						if(!(Arrays.equals(a.get(i), b.get(i))))
							return false;
					}
				}
			}
		return true;
	}
	
	protected void setCurrentView(View view) {
		currentView = view;
	}
	
	protected void setMaster(int master) {
		this.master = master;
	}
	
	protected int getMaster() {
		return master;
	}
	
	protected void setInstallingState(boolean installing) {
		this.installingState = installing;
	}

        private class SpeculativeThread extends Thread {

                private final int clientId;
                private long nextOp;
                private boolean doWork;
                private boolean paused;
                private final Object opSync;
                private final Object pauseSync;
                
                private SpeculativeThread(int clientId, long initOp) {
                    
                    this.clientId = clientId;
                    this.nextOp = initOp;
                    this.doWork = true;                    
                    this.paused = false;                    
                    this.opSync = new Object();
                    this.pauseSync = new Object();

                }

                @Override
                public void run() {

                    while (this.doWork) {

                        ConnManager connManager = sm.getConnManager(this.clientId);
                        
                        if (!connManager.isConnected()) { // avoid a null pointer exception
                            try {
                                Thread.sleep(100);
                                continue;
                            } catch (InterruptedException ex) {
                                java.util.logging.Logger.getLogger(MessageProcessor.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }

                        // this synchronization is needed because ConnManager.pollMessage() is blocking
                        // and can lead to a deadlock between this thread and the waitForOps command
                        synchronized(pauseSync) {
                            while (this.paused) {
                                logger.debug("----[SpecThread] Client #" + this.clientId + " paused spec thread via commit/rollback op");
                                try {
                                    pauseSync.wait();
                                } catch (InterruptedException ex) {
                                    java.util.logging.Logger.getLogger(MessageProcessor.class.getName()).log(Level.SEVERE, null, ex);
                                }

                            }
                        }
                        
                        synchronized(opSync) {
                        
                            logger.debug("----[SpecThread] Client #" + this.clientId + " waiting for operations #" + this.nextOp + " and #" + (this.nextOp + 1));
                            Message[] op = connManager.pollMessages(this.nextOp , 2);

                            logger.debug("----[SpecThread] Client #" + this.clientId + " obtained operations #" + this.nextOp + " and #" + (this.nextOp + 1));
                            logger.debug("----[SpecThread] Client #" + this.clientId + " waiting for commit #" + op[1].getLastCommitedTransId());

                            waitforCommit(op[1].getLastCommitedTransId(), this.clientId);
                            logger.debug("----[SpecThread] Client #" + this.clientId + " reached commit #" + op[1].getLastCommitedTransId());

                            if (op[1].getOpcode() != OpcodeList.ROLLBACK_SEND) {

                                byte[] replyHash = executeOperation(op[0], this.clientId);
                                results.get(this.clientId).add(replyHash);       
                                logger.debug("----[SpecThread] Client #" + this.clientId + " executed op #" + op[0].getOpSequence());
                            }
                            else {
                                
                                // since the transaction might be rolling back due to a conflict at the master,
                                // the operation cannot safely execute. Moreover, the hash cannot be deterministically verified either
                                results.get(this.clientId).add(new byte[0]);       
                                logger.debug("----[SpecThread] Client #" + this.clientId + " transaction is rolling back after op #" + op[0].getOpSequence() + ", skipping execution");                                
                            }
                            
                            if (op[1].getOpcode() != OpcodeList.COMMIT && op[1].getOpcode() != OpcodeList.ROLLBACK_SEND) {
                                connManager.enqueueOperation(op[1]);
                                this.nextOp++;
                            }
                            else {
                                this.nextOp = (op[1].getOpSequence() + 1) ;
                                this.paused = true;
                            }
                            opSync.notify();
                        }

                    }
                }
                
                public void waitforCommit(long transId, int clientId) {
                    synchronized(this) {
                        while (transId > lastCommitedTransId) {

                                logger.debug("---- Client " + clientId + " is waiting for commit #" + transId + " (currently at #" + lastCommitedTransId + ")");
                            try {
                                this.wait();
                            } catch (InterruptedException ex) {
                                java.util.logging.Logger.getLogger(SessionManager.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                    }
                    logger.debug("---- Client " + clientId + " reached commit #" + lastCommitedTransId + " (currently at #" + lastCommitedTransId + ")");
                }
                public void waitForOps(long sequence) {
                    synchronized(opSync) {
                        try {
                            while (this.nextOp < sequence) {
                                logger.debug("---- Client #" + this.clientId + " waiting to reach op sequence #" + sequence);
                                opSync.wait();

                            }
                        } catch (InterruptedException ex) {
                            java.util.logging.Logger.getLogger(MessageProcessor.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    logger.debug("---- Client #" + clientId + " reached op sequence #" + sequence);
                }
                
                public void resumeSpec() {
                    synchronized(pauseSync) {
                        paused = false; 
                        pauseSync.notify();
                    }
                    logger.debug("----[SpecThread] Client #" + this.clientId + " resumed spec thread");
                }
                public void shutDown() {
                        logger.debug("---- Client #" + clientId + " shuting down spec thread");
                    doWork = false;
                }
        }
}
