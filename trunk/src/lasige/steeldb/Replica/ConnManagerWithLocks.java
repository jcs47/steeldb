package lasige.steeldb.Replica;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import lasige.steeldb.comm.Message;

public class ConnManagerWithLocks {
	
	private Connection conn;
	private Queue<Message> transQueue;
	private boolean readWriteTransaction;
	private boolean commitingTransaction;
	/**
	 * This semaphore is used to force the queries to be executed
	 * only after the connection was successfully created.
	 */
	private	Lock connectionLock = new ReentrantLock();
	private Condition correctState = connectionLock.newCondition();
	private int currentOperationId;
	private AtomicInteger operationCount;
	
	private static final int CONNECTION_TIMEOUT = 10;
	
    private Logger logger = Logger.getLogger("steeldb_replica");
	
	protected ConnManagerWithLocks() {
		transQueue = new LinkedList<Message>();
		readWriteTransaction = false;
		commitingTransaction = false;
		operationCount = new AtomicInteger();
	}
	
	protected boolean connect(String url, String user, String pass) {
		try {
			connectionLock.lock();
			url += ";MVCC=TRUE;LOCK_TIMEOUT=" + CONNECTION_TIMEOUT;
			logger.info("CONNECTING TO DATABASE. URL: " + url);
			conn = DriverManager.getConnection(url, user, pass);
			currentOperationId = operationCount.incrementAndGet();
			correctState.signalAll();
			connectionLock.unlock();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn != null;
	}
	
	public void enqueueOperation(Message m) {
		connectionLock.lock();
		logger.debug("enqueueOperation. currentOperationId: " + currentOperationId + ", operationId: " + m.getOperationId());
		while(m.getOperationId() != currentOperationId) {
			correctState.awaitUninterruptibly();
		}
		transQueue.offer(m);
		currentOperationId = operationCount.incrementAndGet();
		correctState.signalAll();
		connectionLock.unlock();
	}

	public Queue<Message> getTrans() {
		return transQueue;
	}

	public void clearTransaction() {
		transQueue = new LinkedList<Message>();
	}

	protected void closeConn() throws SQLException {
		conn.close();
	}
	
	/**
	 * Verifies is the connection is not set as autocommit.
	 * If not, updates the transaction from readonly to read-write.
	 * Should be called by methods that changes the state of the database
	 * (INSERT, UPDATE or DELETE) queries.
	 */
	protected void readWriteTransaction(int operationId) throws SQLException {
		connectionLock.lock();
		logger.debug("readWriteTransaction. currentOperationId: " + currentOperationId + ", operationId: " + operationId);
		while(operationId != currentOperationId) {
			correctState.awaitUninterruptibly();
		}
		if(!conn.getAutoCommit())
			readWriteTransaction = true;
		connectionLock.unlock();
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
		return conn.createStatement();
	}
	
	protected DatabaseMetaData getMetaData(int operationId) throws SQLException {
		connectionLock.lock();
		logger.debug("getMetaData. currentOperationId: " + currentOperationId + ", operationId: " + operationId);
		while(operationId != currentOperationId) {
			try {
				correctState.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		DatabaseMetaData returnValue = conn.getMetaData();
		currentOperationId = operationCount.incrementAndGet();
		correctState.signalAll();
		connectionLock.unlock();
		return returnValue;
	}
	
	protected void setAutoCommit(boolean autoCommit, int operationId) throws SQLException {
		connectionLock.lock();
		logger.debug("setAutoCommit. currentOperationId: " + currentOperationId + ", operationId: " + operationId);
		while(currentOperationId != operationId) {
			try {
				correctState.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		conn.setAutoCommit(autoCommit);
		if(autoCommit)
			this.readWriteTransaction = false;
		currentOperationId = operationCount.incrementAndGet();
		correctState.signalAll();
		connectionLock.unlock();
	}
	
	protected void commit(int operationId) throws SQLException {
		connectionLock.lock();
		logger.debug("commit. currentOperationId: " + currentOperationId + ", operationId: " + operationId);
		while(currentOperationId != operationId) {
			try {
				correctState.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		this.commitingTransaction = false;
		this.readWriteTransaction = false;
		conn.commit();
		currentOperationId = operationCount.incrementAndGet();
		correctState.signalAll();
		connectionLock.unlock();
	}

	protected void rollback(int operationId) throws SQLException {
		connectionLock.lock();
		logger.debug("rollback. currentOperationId: " + currentOperationId + ", operationId: " + operationId);
		while(currentOperationId != operationId) {
			try {
				correctState.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		this.commitingTransaction = false;
		this.readWriteTransaction = false;
		conn.rollback();
		currentOperationId = operationCount.incrementAndGet();
		correctState.signalAll();
		connectionLock.unlock();
	}
	
	protected void initQueryExecution(int operationId) {
		connectionLock.lock();
		logger.debug("initQueryExecution. currentOperationId: " + currentOperationId + ", operationId: " + operationId);
		while(currentOperationId != operationId) {
			try {
				correctState.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		connectionLock.unlock();
	}
	
	protected void finishQueryExecution(int operationId) {
		connectionLock.lock();
		currentOperationId = operationCount.incrementAndGet();
		correctState.signalAll();
		connectionLock.unlock();
	}
	
	/**
	 * This method had to be created to force the message comparisons to
	 * wait until all messages for the transaction has been enqueued. The
	 * problem happened when a commit message arrived but all the updates
	 * or queries inside a readWrite transaction hasn't arrive yet. The
	 * commit method tried to compare the operation array with the commit
	 * array and they didn't match. Now the replica waits until the operation
	 * expected is the commit
	 * 
	 * @param operationId
	 */
	protected void waitForSync(int operationId) {
		connectionLock.lock();
		logger.debug("waitforSync. currentOperationId: " + currentOperationId + ", operationId: " + operationId);
		while(operationId != currentOperationId) {
			correctState.awaitUninterruptibly();
		}
		connectionLock.unlock();
	}
}
