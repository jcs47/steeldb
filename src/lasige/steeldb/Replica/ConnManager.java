package lasige.steeldb.Replica;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Queue;
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

public class ConnManager {
	
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
	
    private Logger logger = Logger.getLogger("steeldb_replica");
	
	protected ConnManager() {
		transQueue = new LinkedList<Message>();
		readWriteTransaction = false;
		commitingTransaction = false;
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
		transQueue.offer(m);
	}

	
	public void setTransactionQueue(Queue<Message> transQueue) {
		this.transQueue = transQueue;
	}

	public Queue<Message> getTransactionQueue() {
		return transQueue;
	}

	public void clearTransaction() {
		transQueue = new LinkedList<Message>();
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
		transQueue = new LinkedList<Message>();
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
