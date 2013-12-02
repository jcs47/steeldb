package lasige.steeldb.Replica;

import java.io.Serializable;
import java.util.Set;

import lasige.steeldb.statemanagement.DBConnectionParams;

class DBCheckpoint implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1816866207237041962L;
	
	private final byte[] dump;
	private final Set<DBConnectionParams> connections;
	
	DBCheckpoint(byte[] dump, Set<DBConnectionParams> connections) {
		this.dump = dump;
		this.connections = connections;
	}

	protected byte[] getDump() {
		return dump;
	}
	protected Set<DBConnectionParams> getConnections() {
		return connections;
	}

}
