package lasige.steeldb.comm;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * 
 * @author Marcel Santos
 *
 */
public class FinishTransactionRequest implements Serializable {
	
	private static final long serialVersionUID = -3535187786994154985L;
	
	private final LinkedList<byte[]> resHashes;
	private final LinkedList<Message> operations;
	
	public FinishTransactionRequest(LinkedList<byte[]> resHashes, LinkedList<Message> operations) {
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
