package lasige.steeldb.statemanagement;

import org.apache.log4j.Logger;

import bftsmart.reconfiguration.ServerViewManager;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.strategy.StandardSMMessage;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.util.TOMUtil;

public class StateMessageProcessor extends Thread {

	private TOMLayer tomLayer;
	private Recoverable recoverable;
	private StandardSMMessage request;
	private int me;
	private ServerViewManager SVManager;
	
    private Logger logger = Logger.getLogger("steeldb_replica");
    private boolean sendState = false;

    public StateMessageProcessor(TOMLayer tomLayer, Recoverable recoverable, StandardSMMessage request) {
		this.tomLayer = tomLayer;
		this.recoverable = recoverable;
		this.request = request;
		SVManager = tomLayer.reconfManager;
		me = SVManager.getStaticConf().getProcessId();
		if(request.getReplica() == me)
			sendState = true;
	}
	
	public void run() {
        ApplicationState thisState = recoverable.getState(request.getEid(), sendState);
        if (thisState == null) {
          thisState = recoverable.getState(-1, sendState);
        }
        int[] targets = { request.getSender() };
        SMMessage smsg = new StandardSMMessage(me, request.getEid(), TOMUtil.SM_REPLY,
        		-1, thisState, SVManager.getCurrentView(), -1, tomLayer.lm.getCurrentLeader());
        logger.debug("Sending state");
        tomLayer.getCommunication().send(targets, smsg);
        logger.debug("Sent");
	}
}
