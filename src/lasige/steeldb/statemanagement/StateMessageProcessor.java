package lasige.steeldb.statemanagement;

import org.apache.log4j.Logger;

import bftsmart.reconfiguration.ServerViewController;
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
	private ServerViewController SVController;
	
    private Logger logger = Logger.getLogger("steeldb_replica");
    private boolean sendState = false;

    public StateMessageProcessor(TOMLayer tomLayer, Recoverable recoverable, StandardSMMessage request) {
		this.tomLayer = tomLayer;
		this.recoverable = recoverable;
		this.request = request;
		SVController = tomLayer.controller;
		me = SVController.getStaticConf().getProcessId();
		if(request.getReplica() == me)
			sendState = true;
	}
	
	public void run() {
        ApplicationState thisState = recoverable.getState(request.getCID(), sendState);
        if (thisState == null) {
          thisState = recoverable.getState(-1, sendState);
        }
        int[] targets = { request.getSender() };
        SMMessage smsg = new StandardSMMessage(me, request.getCID(), TOMUtil.SM_REPLY,
        		-1, thisState, SVController.getCurrentView(), -1, tomLayer.execManager.getCurrentLeader());
        logger.debug("Sending state");
        tomLayer.getCommunication().send(targets, smsg);
        logger.debug("Sent");
	}
}
