package lasige.steeldb.client;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import bftsmart.communication.client.ReplyListener;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.util.Extractor;

public class SteelDBListener implements ReplyListener {

	private ReentrantLock canReceiveLock;
	private Comparator<byte[]> comparator;
	private Extractor extractor;
	private TOMMessage response;
	private Semaphore sm;
	private final Map<Integer, TOMMessage> replies;
	// To sign that the client already received the expected
	// matching replies and discard the unnecessary
	private boolean hasResult;
	private final int master;

	private Logger logger = Logger.getLogger("steeldb_client");

	public SteelDBListener(byte[] request, Comparator<byte[]> comparator, Extractor extractor) {
		this(request, comparator, extractor, 0);
	}

	public SteelDBListener(byte[] request, Comparator<byte[]> comparator, Extractor extractor, int master) {
		this.comparator = comparator;
		this.extractor = extractor;
		canReceiveLock = new ReentrantLock();
		replies = new HashMap<Integer, TOMMessage>();
		response = null;
		sm = new Semaphore(0);
		this.master = master;
	}

	public TOMMessage getResponse() {
		if(response != null)
			return response;
		try {
			if (!this.sm.tryAcquire(5, TimeUnit.SECONDS)) {
				logger.error("Couldn't get reply from server");
				return null;
			}
		} catch (InterruptedException ex) {
			logger.error(ex.getMessage());
		} 	
//		logger.debug("Response extracted = " + response);
		return response;
	}

	public void replyReceived(TOMMessage reply) {
		if(hasResult)
			return;
//		logger.debug("waiting for canREceiveLock");
		canReceiveLock.lock();
//		logger.debug("canReceive lock granted");
//		logger.debug("Receiving reply from " + reply.getSender() + " with reqId:" + reply.getSequence());
		if(reply.getSender() == master) {
			response = reply;
		}
//		logger.debug("response:" + response);
		hasResult = true;
		this.sm.release();
		canReceiveLock.unlock();
	}

}
