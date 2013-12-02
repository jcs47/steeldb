package lasige.steeldb.comm;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * 
 * @author Marcel Santos
 *
 */
public class MasterChangeRequest implements Serializable {
	
	private static final long serialVersionUID = -2483341216649336043L;

	private final LinkedList<byte[]> resHashes;
	private final LinkedList<Message> operations;
	
	public MasterChangeRequest(LinkedList<byte[]> resHashes, LinkedList<Message> operations) {
		this.resHashes = resHashes;
		this.operations = operations;
	}

	public LinkedList<byte[]> getResHashes() {
		return resHashes;
	}

	public LinkedList<Message> getOperations() {
		return operations;
	}
	
}
