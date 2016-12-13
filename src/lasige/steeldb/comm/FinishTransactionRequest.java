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
	//private final LinkedList<Message> operations;
        private final long firstOpSequence;
	
	public FinishTransactionRequest(LinkedList<byte[]> resHashes, long firstOpSequence) {
		this.resHashes = resHashes;
		//this.operations = operations;
                this.firstOpSequence = firstOpSequence;
	}

	public LinkedList<byte[]> getResHashes() {
		return resHashes;
	}

	/*public LinkedList<Message> getOperations() {
		return operations;
	}*/
	
        public long getFirstOpSequence() {
            return firstOpSequence;
        }
}
