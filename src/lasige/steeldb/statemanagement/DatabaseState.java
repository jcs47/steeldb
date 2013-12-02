package lasige.steeldb.statemanagement;

import java.io.Serializable;
import java.util.List;

/**
 * 
 * @author Marcel Santos
 *
 */
public class DatabaseState implements Serializable {
	
	private static final long serialVersionUID = 4334002605711037339L;
	
	private final byte[] state;
	private final byte[] stateHash;
	private final List<DBConnectionParams> connections;
	private final int master;
	
	public byte[] getState() {
		return state;
	}

	public byte[] getStateHash() {
		return stateHash;
	}

	public boolean hasState() {
		return state != null;
	}
	
	public int getMaster() {
		return master;
	}

	public List<DBConnectionParams> getConnections() {
		return connections;
	}

	public DatabaseState(byte[] state, byte[] stateHash, List<DBConnectionParams> connections, int master) {
		this.state = state;
		this.stateHash = stateHash;
		this.connections = connections;
		this.master = master;
	}
}
