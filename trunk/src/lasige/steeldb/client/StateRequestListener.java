package lasige.steeldb.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import bftsmart.communication.client.ReplyListener;
import bftsmart.tom.core.messages.TOMMessage;

public class StateRequestListener implements ReplyListener {

    private ReentrantLock canReceiveLock;
    private int receivedReplies;
    private TOMMessage response;
    private Semaphore sm;
    private final int stateReplica; // the replica expected to send the state
    private final List<TOMMessage> replies;
    // To sign that the client already received the expected
    // matching replies and discard the unnecessary
    private boolean expectedReplied;
    
    private Logger logger = Logger.getLogger("steeldb_client");

    public StateRequestListener(byte[] request, int stateReplica) {
    	canReceiveLock = new ReentrantLock();
    	replies = new ArrayList<TOMMessage>();
    	this.stateReplica = stateReplica;
    	receivedReplies = 0;
    	response = null;
    	sm = new Semaphore(0);
    }

    public TOMMessage getResponse() {
    	if(response != null)
           	return response;
        try {
            if (!this.sm.tryAcquire(20, TimeUnit.SECONDS)) {
                logger.debug("Couldn't get reply from server");
                return null;
            }
        } catch (InterruptedException ex) {
        	logger.error(ex.getMessage());
        }
        logger.debug("Response extracted = " + response);
       	return response;
    }
	
	public void replyReceived(TOMMessage reply) {
        canReceiveLock.lock();
        logger.debug("Receiving reply from " + reply.getSender() + ". Expected sender replica: " + stateReplica);
		replies.add(reply);
    	receivedReplies++;
    	if(reply.getSender() == stateReplica)
    		expectedReplied = true;
    	
        if(receivedReplies >= 2 && expectedReplied) {
        	response = replies.get(0);
            this.sm.release(); // resumes the thread that is executing the "invoke" method
    	}
        canReceiveLock.unlock();
	}

}
