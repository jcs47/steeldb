package lasige.steeldb.Replica;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Level;
//import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import lasige.steeldb.comm.Message;
import lasige.steeldb.statemanagement.DBConnectionParams;

/**
 * @author mhsantos
 * 
 * This class manages the connection for a client, i.e., a JDBC connection
 * between one client and the database. It assumes that the requests are
 * delivered in FIFO order, so there is no need for locks to manage the ordering
 * of the messages being executed.
 * It is also assumed that the replica that delivers the code to MessageProcessor
 * and then to ConnManager extends FIFOExecutableRecoverable, which enforces the
 * ordering of the messages to be executed in FIFO order.
 */

public class ConnManager extends Thread {
	
        private int clientId;
	private Connection conn;
	private Queue<Message> transQueue;
	private DBConnectionParams login;
	private boolean readWriteTransaction;
	private boolean commitingTransaction;
	private boolean aborted = false;
	private boolean connected = false;
	// forces all operations to wait for connect
//	private CountDownLatch connectedLatch;
	
	private static final int CONNECTION_TIMEOUT = 5000;
        private Object queueSync;
	
        private Logger logger = Logger.getLogger("steeldb_processor");
	
	protected ConnManager(int clientId) {
                this.clientId = clientId;
		transQueue = new PriorityBlockingQueue<>();
		readWriteTransaction = false;
		commitingTransaction = false;
                queueSync = new Object();
//		connectedLatch = new CountDownLatch(1);
	}
	
	protected boolean connect(String url, String user, String pass) {
		try {
//			url += ";MVCC=TRUE;LOCK_TIMEOUT=" + CONNECTION_TIMEOUT; // h2 specific
//			logger.info("CONNECTING TO DATABASE. URL: " + url);
			conn = DriverManager.getConnection(url, user, pass);
			login = new DBConnectionParams(null, user, pass);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if(conn != null) {
			connected = true;
//			connectedLatch.countDown();
		}
		return conn != null;
	}
		
	public void enqueueOperation(Message m) {
            
                synchronized(queueSync) {
                    
                    transQueue.offer(m);
                    logger.debug("Offered message #" + m.getOpSequence() + " from client " + m.getClientId());
                    queueSync.notify();
                }
	}

	
	public void setTransactionQueue(Queue<Message> transQueue) {
		this.transQueue = transQueue;
	}

	public Queue<Message> getTransactionQueue() {
		return transQueue;
	}
                
        public Message[] pollMessages(long firstOpSequence, int numMessages) {
            
            if (firstOpSequence == -1) return new Message[0];
            Message[] ret = new Message[numMessages];
            
            synchronized(queueSync) {
            
                for (int i = 0; i < numMessages; i++) {

                    long sequence = firstOpSequence + i;
                    while (transQueue.peek() == null ||
                            transQueue.peek().getOpSequence() != sequence) {
                        logger.debug("---- Waiting for operation #" + sequence + 
                                 " from client " + clientId + (transQueue.peek() != null ? " currently at #" + transQueue.peek().getOpSequence() : ""));
                        try {
                            queueSync.wait();
                        } catch (InterruptedException ex) {
                            java.util.logging.Logger.getLogger(ConnManager.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
        
                    ret[i] = transQueue.poll();
                    logger.debug("Polled message #" + ret[i].getOpSequence() + " from client " + ret[i].getClientId());

                }
            }
            
            return ret;
        }
        
	public void clearTransaction() {
		transQueue = new PriorityBlockingQueue<>();
	}

	protected void closeConn() throws SQLException {
		conn.close();
		connected = false;
//		connectedLatch = new CountDownLatch(1);
	}
	
	/**
	 * Verifies is the connection is not set as autocommit.
	 * If not, updates the transaction from readonly to read-write.
	 * Should be called by methods that changes the state of the database
	 * (INSERT, UPDATE or DELETE) queries.
	 */
	protected void readWriteTransaction() throws SQLException {
		if(!conn.getAutoCommit())
			readWriteTransaction = true;
	}
	
	protected boolean isReadWriteTransaction() {
		boolean returnValue = readWriteTransaction;
		return returnValue;
	}

	protected boolean isCommitingTransaction() {
		boolean returnValue = commitingTransaction;
		return returnValue;
	}

	protected void setCommitingTransaction(boolean commitingTransaction) {
		this.commitingTransaction = commitingTransaction;
		if(!commitingTransaction)
			this.readWriteTransaction = false;
	}

	protected Statement createStatement() throws SQLException {
//		try {
//			connectedLatch.await();
//		} catch (InterruptedException e) {
//			logger.error("Error on createStatement", e);
//		}
		return conn.createStatement();
	}
	
        protected Statement createStatement(int resultSetType,
                                 int resultSetConcurrency)
                                   throws SQLException {
            
            return conn.createStatement(resultSetType, resultSetConcurrency);
            
        }
        
	protected DatabaseMetaData getMetaData() throws SQLException {
		logger.debug("conn is not null " +  (conn != null));
//		try {
//			connectedLatch.await();
//		} catch (InterruptedException e) {
//			logger.error("Error getting metadata", e);
//		}
		DatabaseMetaData returnValue = conn.getMetaData();
		return returnValue;
	}
	
	protected void setAutoCommit(boolean autoCommit) throws SQLException {
		conn.setAutoCommit(autoCommit);
		if(autoCommit)
			this.readWriteTransaction = false;
		this.commitingTransaction = false;
	}
	
	protected void commit() throws SQLException {
		this.commitingTransaction = false;
		this.readWriteTransaction = false;
		conn.commit();
	}

	protected void rollback() throws SQLException {
		reset();
		conn.rollback();
	}
	
	protected void abort() {
		aborted = true;
	}
	
	protected boolean isAborted() {
		return aborted;
	}
	
	/**
	 * Reset a connection after a master change. In non master replicas as operations
	 * are not executed against the database it is necessary only to clean the
	 * operation list and set the flags to the original state.
	 */
	protected void reset() {
		//transQueue = new PriorityBlockingQueue<>();
		readWriteTransaction = false;
		commitingTransaction = false;
		aborted = false;
	}
	
	protected boolean isAutoCommit() {
		boolean auto = true;
		try {
			auto = conn.getAutoCommit();
		} catch (SQLException e) {
			logger.error("error in getautocommi", e);
		}
		return auto;
	}
	
	protected DBConnectionParams getLogin() {
		return this.login;
	}
	
	protected boolean isConnected() {
		return connected;
	}
}
