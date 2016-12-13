package lasige.steeldb.Replica;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import lasige.steeldb.comm.Message;
import lasige.steeldb.comm.OpcodeList;
import lasige.steeldb.statemanagement.DBConnectionParams;
import lasige.steeldb.statemanagement.DatabaseState;
import lasige.steeldb.statemanagement.DatabaseStateManager;

import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.StateManager;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.util.Storage;
import bftsmart.tom.util.TOMUtil;

import org.apache.log4j.Logger;
import org.h2.tools.RunScript;
import org.h2.tools.Script;

public class Replica extends DefaultRecoverable {

	private int id;
        private ServiceReplica replica;
	private MessageProcessor mProcessor = null;
	private ConnectionParams connParams = null;
	//private ReplicaContext replicaContext;
	
    //private StateManager stateManager;
    
    //private Storage storeBatchLatency = new Storage(20000);
    //private Storage storeBatchSize = new Storage(20000);

    //private Storage totalLatency = new Storage(20000);
    //private Storage deliveryLatency = new Storage(20000);
    //private Storage posConsLatency = new Storage(20000);
    //private Storage execLatency = new Storage(20000);

    private Logger logger = Logger.getLogger("steeldb_replica");
    
    int count = 0;
    
    public Replica(int id, Properties config) {
        
		super();
		connParams = new ConnectionParams(config);
		mProcessor = new MessageProcessor(id, connParams.getDriver(), connParams.getUrl());
                this.id = id;
		replica = new ServiceReplica(id,this,this);                
                View view = replica.getReplicaContext().getCurrentView();
		mProcessor.setCurrentView(view);
		logger.info("Replica started");
	}
        
        @Override
        public byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtxs) {

            //storeBatchSize.store(commands.length);
            
            //long init = System.nanoTime();

            byte [][] replies = new byte[commands.length][];
            for (int i = 0; i < commands.length; i++) {
                //if(msgCtxs != null && msgCtxs[i] != null) {
                replies[i] = executeSingle(commands[i],msgCtxs[i]);
                //}
                //else executeSingle(commands[i],null);
            }
            //long latency = (System.nanoTime() - init) / 1000000;
            //storeBatchLatency.store(latency);

            /*if (storeBatchLatency.getCount() > 0 && storeBatchLatency.getCount() % 100 == 0) {
                System.out.println("Replica // Average latency for " +  storeBatchLatency.getCount() + " RSM batches = " + storeBatchLatency.getAverage(false) + " ms ");
                System.out.println("Replica // Standard desviation for " + storeBatchLatency.getCount() + " RSM batches = " + storeBatchLatency.getDP(false));
                System.out.println("Replica // Maximum latency for " + storeBatchLatency.getCount() + " RSM batches = " + storeBatchLatency.getMax(false) + " ms ");
                System.out.println("Replica // Median for " + storeBatchLatency.getCount() + " RSM batches = " + storeBatchLatency.getPercentile(0.5) + " ms ");
                System.out.println("Replica // 90th for " + storeBatchLatency.getCount() + " RSM batches = " + storeBatchLatency.getPercentile(0.9) + " ms ");
                System.out.println("Replica // 95th for " + storeBatchLatency.getCount() + " RSM batches = " + storeBatchLatency.getPercentile(0.95) + " ms ");
                System.out.println("Replica // 99th for " + storeBatchLatency.getCount() + " RSM batches = " + storeBatchLatency.getPercentile(0.99) + " ms ");

                System.out.println("\nReplica // Average size for " +  storeBatchSize.getCount() + " RSM batches = " + storeBatchSize.getAverage(false) + " messages ");
                System.out.println("Replica // Standard desviation for " + storeBatchSize.getCount() + " RSM batches = " + storeBatchSize.getDP(false));
                System.out.println("Replica // Maximum size for " + storeBatchSize.getCount() + " RSM batches = " + storeBatchSize.getMax(false) + " messages ");
                System.out.println("Replica // Median for " + storeBatchSize.getCount() + " RSM batches = " + storeBatchSize.getPercentile(0.5) + " messages ");
                System.out.println("Replica // 90th for " + storeBatchSize.getCount() + " RSM batches = " + storeBatchSize.getPercentile(0.9) + " messages ");
                System.out.println("Replica // 95th for " + storeBatchSize.getCount() + " RSM batches = " + storeBatchSize.getPercentile(0.95) + " messages ");
                System.out.println("Replica // 99th for " + storeBatchSize.getCount() + " RSM batches = " + storeBatchSize.getPercentile(0.99) + " messages ");

            }*/
            return replies;
        }
                
	private byte[] executeSingle(byte[] command, MessageContext msgCtx) {
            
            msgCtx.getFirstInBatch().executedTime = System.nanoTime();
                                    
            byte [] reply = null;
		
		Message m = Message.getMessage(command);
		Message replyMsg = null;
		
		logger.debug(":::: Client: " + m.getClientId() + ", #" + m.getOpSequence() + ", ordered, " + m.getContents());
		
		switch(m.getOpcode()) {
		/*case OpcodeList.GET_STATE:
			SMMessage smmsg = (SMMessage)m.getContents();
			if(smmsg.getSender() != id)
				stateManager.SMRequestDeliver(smmsg, true);
			replyMsg = new Message(OpcodeList.STATE_REQUESTED, null, false, mProcessor.getMaster());
			break;*/
		case OpcodeList.EXECUTE:
			replyMsg = mProcessor.processExecute(m, m.getClientId());
			break;
		case OpcodeList.EXECUTE_BATCH:
			replyMsg = mProcessor.processExecuteBatch(m, m.getClientId());
			break;
		case OpcodeList.EXECUTE_QUERY:
			replyMsg = mProcessor.processExecuteQuery(m, m.getClientId());
			break;
		case OpcodeList.EXECUTE_UPDATE:
			replyMsg = mProcessor.processExecuteUpdate(m, m.getClientId());
			break;
		case OpcodeList.LOGIN_SEND:
			replyMsg = mProcessor.processLogin(m, m.getClientId());
			break;
		case OpcodeList.COMMIT_AUTO:
			replyMsg = mProcessor.processAutoCommit(m, m.getClientId());
			break;
		case OpcodeList.COMMIT:
			replyMsg = mProcessor.processCommit(m, m.getClientId());
			break;
		case OpcodeList.ROLLBACK_SEND:
			replyMsg = mProcessor.processRollback(m, m.getClientId());
			break;
		case OpcodeList.MASTER_CHANGE:
			replyMsg = mProcessor.processMasterChange(m);
			break;
		case OpcodeList.SET_FETCH_SIZE:
			replyMsg = mProcessor.processSetFetchSize(m, m.getClientId());
			break;
		case OpcodeList.SET_MAX_ROWS:
			replyMsg = mProcessor.processSetMaxRows(m, m.getClientId());
			break;
		case OpcodeList.CLOSE:
			replyMsg = mProcessor.processClose(m, m.getClientId());
			break;
		case OpcodeList.GET_DB_METADATA:
			int clientId = m.getClientId();
			replyMsg = mProcessor.getDBMetadata(clientId);
			break;
		default:
			logger.debug("[Replica "+this.id+"] Unknown opcode received: " + m.getOpcode());
		}
		
		reply = replyMsg.getBytes();

                /*long fin = System.nanoTime();
                execLatency.store((fin - msgCtx.getFirstInBatch().executedTime) / 1000000);
                totalLatency.store((fin - msgCtx.getFirstInBatch().decisionTime) / 1000000);
                deliveryLatency.store((msgCtx.getFirstInBatch().executedTime - msgCtx.getFirstInBatch().decisionTime) / 1000000);
                posConsLatency.store((msgCtx.getFirstInBatch().deliveryTime - msgCtx.getFirstInBatch().decisionTime) / 1000000);

                if (posConsLatency.getCount() > 0 && posConsLatency.getCount() % 100 == 0) {

                    System.out.println("Replica // Average latency for " +  totalLatency.getCount() + " RSM total = " + totalLatency.getAverage(false) + " ms ");
                    System.out.println("Replica // Standard desviation for " + totalLatency.getCount() + " RSM total = " + totalLatency.getDP(false));
                    System.out.println("Replica // Maximum latency for " + totalLatency.getCount() + " RSM total = " + totalLatency.getMax(false) + " ms ");
                    System.out.println("Replica // Median for " + totalLatency.getCount() + " RSM total = " + totalLatency.getPercentile(0.5) + " ms ");
                    System.out.println("Replica // 90th for " + totalLatency.getCount() + " RSM total = " + totalLatency.getPercentile(0.9) + " ms ");
                    System.out.println("Replica // 95th for " + totalLatency.getCount() + " RSM total = " + totalLatency.getPercentile(0.95) + " ms ");
                    System.out.println("Replica // 99th for " + totalLatency.getCount() + " RSM total = " + totalLatency.getPercentile(0.99) + " ms ");
                    
                    System.out.println("Replica // Average latency for " +  posConsLatency.getCount() + " RSM pos-cons = " + posConsLatency.getAverage(false) + " ms ");
                    System.out.println("Replica // Standard desviation for " + posConsLatency.getCount() + " RSM pos-cons = " + posConsLatency.getDP(false));
                    System.out.println("Replica // Maximum latency for " + posConsLatency.getCount() + " RSM pos-cons = " + posConsLatency.getMax(false) + " ms ");
                    System.out.println("Replica // Median for " + posConsLatency.getCount() + " RSM pos-cons = " + posConsLatency.getPercentile(0.5) + " ms ");
                    System.out.println("Replica // 90th for " + posConsLatency.getCount() + " RSM pos-cons = " + posConsLatency.getPercentile(0.9) + " ms ");
                    System.out.println("Replica // 95th for " + posConsLatency.getCount() + " RSM pos-cons = " + posConsLatency.getPercentile(0.95) + " ms ");
                    System.out.println("Replica // 99th for " + posConsLatency.getCount() + " RSM pos-cons = " + posConsLatency.getPercentile(0.99) + " ms ");

                    System.out.println("Replica // Average latency for " +  deliveryLatency.getCount() + " RSM delivery = " + deliveryLatency.getAverage(false) + " ms ");
                    System.out.println("Replica // Standard desviation for " + deliveryLatency.getCount() + " RSM delivery = " + deliveryLatency.getDP(false));
                    System.out.println("Replica // Maximum latency for " + deliveryLatency.getCount() + " RSM delivery = " + deliveryLatency.getMax(false) + " ms ");
                    System.out.println("Replica // Median for " + deliveryLatency.getCount() + " RSM delivery = " + deliveryLatency.getPercentile(0.5) + " ms ");
                    System.out.println("Replica // 90th for " + deliveryLatency.getCount() + " RSM delivery = " + deliveryLatency.getPercentile(0.9) + " ms ");
                    System.out.println("Replica // 95th for " + deliveryLatency.getCount() + " RSM delivery = " + deliveryLatency.getPercentile(0.95) + " ms ");
                    System.out.println("Replica // 99th for " + deliveryLatency.getCount() + " RSM delivery = " + deliveryLatency.getPercentile(0.99) + " ms ");

                    System.out.println("Replica // Average latency for " +  execLatency.getCount() + " RSM exec = " + execLatency.getAverage(false) + " ms ");
                    System.out.println("Replica // Standard desviation for " + execLatency.getCount() + " RSM exec = " + execLatency.getDP(false));
                    System.out.println("Replica // Maximum latency for " + execLatency.getCount() + " RSM exec = " + execLatency.getMax(false) + " ms ");
                    System.out.println("Replica // Median for " + execLatency.getCount() + " RSM exec = " + execLatency.getPercentile(0.5) + " ms ");
                    System.out.println("Replica // 90th for " + execLatency.getCount() + " RSM exec = " + execLatency.getPercentile(0.9) + " ms ");
                    System.out.println("Replica // 95th for " + execLatency.getCount() + " RSM exec = " + execLatency.getPercentile(0.95) + " ms ");
                    System.out.println("Replica // 99th for " + execLatency.getCount() + " RSM exec = " + execLatency.getPercentile(0.99) + " ms ");

                }*/

                return reply;
	}

	@Override
	public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
		byte[] replyBytes = null;
		Message reply = null;
		Message m = Message.getMessage(command);

		logger.debug("---- Client: " + m.getClientId() + ", #" + m.getOpSequence() + ", unordered, " + m.getContents());

		switch(m.getOpcode()) {
		case OpcodeList.EXECUTE:
                    if (mProcessor.getMaster() == this.id) {
			reply = mProcessor.processExecute(m, m.getClientId());
                    }
                    else {
                        logger.debug("---- Client: " + m.getClientId() + ", enqueueing EXECUTE #" + m.getOpSequence());
                        //mProcessor.getSessionManager().getConnManager(m.getClientId()).enqueueOperation(m);
                        mProcessor.processOperation(m, m.getClientId());
                    }
                    break;
		case OpcodeList.SET_FETCH_SIZE:
                    break;
		case OpcodeList.EXECUTE_BATCH:
                    if (mProcessor.getMaster() == this.id) {
			reply = mProcessor.processExecuteBatch(m, m.getClientId());
                    }
                    else {
                        logger.debug("---- Client: " + m.getClientId() + ", enqueueing BATCH #" + m.getOpSequence());
                        //mProcessor.getSessionManager().getConnManager(m.getClientId()).enqueueOperation(m);
                        mProcessor.processOperation(m, m.getClientId());
                    }
                    break;
		case OpcodeList.EXECUTE_QUERY:
                    if (mProcessor.getMaster() == this.id) {
			reply = mProcessor.processExecuteQuery(m, m.getClientId());
                    }
                    else {
                        logger.debug("---- Client: " + m.getClientId() + ", enqueueing QUERY #" + m.getOpSequence());
                        //mProcessor.getSessionManager().getConnManager(m.getClientId()).enqueueOperation(m);
                        mProcessor.processOperation(m, m.getClientId());
                    }
                    break;
		case OpcodeList.EXECUTE_UPDATE:
                    if (mProcessor.getMaster() == this.id) {
			reply = mProcessor.processExecuteUpdate(m, m.getClientId());
                    }
                    else {
                        logger.debug("---- Client: " + m.getClientId() + ", enqueueing UPDATE #" + m.getOpSequence());
                        //mProcessor.getSessionManager().getConnManager(m.getClientId()).enqueueOperation(m);
                        mProcessor.processOperation(m, m.getClientId());
                    }
                    break;
		case OpcodeList.COMMIT_AUTO:
                    reply = mProcessor.processAutoCommit(m, m.getClientId());
                    break;
		case OpcodeList.MASTER_CHANGE:
                    reply = mProcessor.processMasterChange(m);
                    break;
		default:
			logger.debug("[Replica "+this.id+"] Unknown opcode received: " + m.getOpcode());
		}
		if(reply != null)
			replyBytes = reply.getBytes();
                else replyBytes = new byte[0]; //bftsmart still has to reply with something
		return replyBytes;
	}

	// Method used for TClouds demo
	/*public byte[] getSnapshotH2() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		String urlWithDB = connParams.getUrl() + connParams.getDatabase();
		try {
			Script.execute(urlWithDB, connParams.getUser(), connParams.getPassword(), baos);
		} catch (SQLException e) {
			logger.error("PROBLEM GETTING DUMP");
			e.printStackTrace();
		}
		byte[] dump = baos.toByteArray();
		logger.debug("Dump taken. Size: " + dump.length + " bytes.");
		List<DBConnectionParams> connParams = mProcessor.getConnections();
		DatabaseState dbState = new DatabaseState(dump, null, connParams, mProcessor.getMaster());
		return TOMUtil.getBytes(dbState);
	}*/

	@Override
	public byte[] getSnapshot() { // To run with Postgres. It does not take the dump
		/*byte[] dump = new byte[0];
		logger.debug("Dump taken. Size: " + dump.length + " bytes.");
		List<DBConnectionParams> connParams = mProcessor.getConnections();
		DatabaseState dbState = new DatabaseState(dump, null, connParams, mProcessor.getMaster());
		return TOMUtil.getBytes(dbState);*/
                return new byte[0];
	}
	
	@Override
	public void installSnapshot(byte[] snapshot) {
		/*StringBuffer dropStatements = new StringBuffer();
//		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.USER CASCADE;\n");
		
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.REPORT CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.APPLICATION_SETTING CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.DTC CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.AUDIT_ACTION CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.CLIENT_ENTITY CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.EVENT CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.DISTRICT CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.DTC_SERVICE CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.CONTROL CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.MUNICIPALITY CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.OPERATIONAL_AREA CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.PERIOD CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.PROFILE CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.SERVICE CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.SPECIAL_DAY CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.SPECIAL_DAY_SERVICE CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.TIMETABLE CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.DTC_STATE CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.USER CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.ALARM CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.SERVICE_STOPWATCH CASCADE;\n");
		dropStatements.append("DROP TABLE IF EXISTS PUBLIC.SERVICE_STOPWATCH_REPORT CASCADE;\n");

		DatabaseState dbState = (DatabaseState)TOMUtil.getObject(snapshot);
		byte[] dump = dbState.getState();
		List<DBConnectionParams> dbParams = dbState.getConnections();
		
		String urlWithDB = connParams.getUrl() + connParams.getDatabase();
		try {
			logger.debug("---INSTALLING SNAPSHOT");
			String ckpFileName = "files/dump_received.sql";
			File tmpckp = new File(ckpFileName);
			FileOutputStream fos = new FileOutputStream(tmpckp);
			fos.write(dropStatements.toString().getBytes());
			fos.flush();
			fos.write(dump);
			fos.flush();
			fos.close();
			
			RunScript.execute(urlWithDB, connParams.getUser(), connParams.getPassword(), ckpFileName, "UTF-8", true);
			mProcessor.setMaster(dbState.getMaster());
			logger.debug("---SNAPSHOT INSTALLED");
		} catch (IOException | SQLException e) {
			logger.error("error installing dump", e);
		}
		
		//Snapshot installed. Will now restore the connections that
		//were opened when the dump was taken.
		logger.debug("--- OPENING CONNECIONS");
		mProcessor.restoreOpenConnections(dbParams, connParams.getDatabase());
		logger.debug("--- OPEN");*/
	}

	/*@Override
	public StateManager getStateManager() {
		if(stateManager == null)
			stateManager = new DatabaseStateManager();
		return stateManager;
	}

	@Override
	public void setReplicaContext(ReplicaContext replicaCtx) {
		this.replicaContext = replicaCtx;
		super.setReplicaContext(replicaCtx);
	}
	
	@Override
    public int setState(ApplicationState recvState) {
		mProcessor.setInstallingState(true);
		int returnValue = super.setState(recvState);
		mProcessor.setInstallingState(false);
		return returnValue;
	}*/

}
