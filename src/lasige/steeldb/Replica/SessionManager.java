package lasige.steeldb.Replica;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import lasige.steeldb.comm.Message;
import lasige.steeldb.statemanagement.DBConnectionParams;

public class SessionManager {

	private HashMap <Integer, ConnManager> clientMap;
	private String urlBase;

	public SessionManager(String url) {
		this.urlBase = url;
		clientMap = new HashMap<Integer,ConnManager>();
	}

	/**
	 * Return a manager to the SQLConnection object.
	 * The connection manager acts as a gateway preventing the
	 * client to directly invoke operations in the database connection.
	 * It is useful to manage the transactions used by the client. 
	 */
	public ConnManager getConnManager(int clientId) {
		ConnManager cManager;
		synchronized(this) {
			cManager = clientMap.get(clientId);
			if(cManager == null) {
				cManager = new ConnManager();
				clientMap.put(clientId, cManager);
			}
		}
		return cManager;
	}
	
	protected void enqueueOperation(Message m,int clientId) {
		ConnManager cManager = clientMap.get(clientId);
		cManager.enqueueOperation(m);
	}
	
	/**
	 * Returns the operation queue for a client and cleans the queue.
	 * @param clientId The client that will execute the transaction operations.
	 * @return The queue of operations.
	 */
	protected Queue<Message> getOperations(int clientId) {
		ConnManager cManager = clientMap.get(clientId);
		Queue<Message> queue = cManager.getTransactionQueue();
		cManager.clearTransaction();
		return queue;	
	}

	protected boolean connect(int clientId, String database, String user, String pass) {
		ConnManager cManager;
		synchronized(this) {
			cManager = clientMap.get(clientId);
			if(cManager == null) {
				cManager = new ConnManager();
				clientMap.put(clientId, cManager);
			}
		}
		boolean connected = cManager.connect(urlBase + database, user, pass);
		if(!connected) {
			clientMap.remove(clientId);
		}
		return connected;
	}

	protected void close(int clientId) throws SQLException {
		ConnManager cManager = clientMap.get(clientId);
		cManager.closeConn();
		clientMap.remove(clientId);
	}
	
	/**
	 * During the checkpoint, the database state is stored in a dump.
	 * Information about open connections must be stored with the dump
	 * in order to the replica that requests the state be able to process
	 * the operations in the log.
	 * @return Set with the connections login properties to open the
	 * connections in the recovering replica.
	 */
	protected List<DBConnectionParams> getConnections() {
		List<DBConnectionParams> connections = new ArrayList<DBConnectionParams>();
		for(Integer key : clientMap.keySet()) {
			ConnManager manager = clientMap.get(key);
			DBConnectionParams login = manager.getLogin();
			login.setClientId(key);
			connections.add(login);
		}
		return connections;
	}
	
	/**
	 * Iterates over the set of connections and mark connections set as read-write
	 * to be aborted in the next operation.
	 * This method should be called when a client suspect the master and request
	 * a master change.
	 */
	protected void abortTransactions() {
		Set<Integer> keys = clientMap.keySet();
		for(Integer clientId : keys) {
			ConnManager conn = clientMap.get(clientId);
			if(conn.isReadWriteTransaction()) {
				conn.abort();
			}
		}
	}
	
	/**
	 * Iterates over the open read-write transactions and gets the queues of pending
	 * operations. This is used in the master change process to execute the pending
	 * operations in the new master.
	 * 
	 * @return A map with the client id and a queue of pending operations.
	 */
	protected Map<Integer, Queue<Message>> getPendingOps() {
		Map<Integer, Queue<Message>> transMap = new TreeMap<Integer, Queue<Message>>();
		for(Integer clientId : clientMap.keySet()) {
			Queue<Message> transQueue = getOperations(clientId);
			if(transQueue.size() > 0) {
				transMap.put(clientId, transQueue);
			}
		}
		return transMap;
	}
}
