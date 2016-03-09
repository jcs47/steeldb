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
import bftsmart.tom.util.TOMUtil;

import org.apache.log4j.Logger;
import org.h2.tools.RunScript;
import org.h2.tools.Script;

public class Replica extends DefaultRecoverable {

	private int id;
        private ServiceReplica replica;
	private MessageProcessor mProcessor = null;
	private ConnectionParams connParams = null;
	private ReplicaContext replicaContext;
	
    private StateManager stateManager;
    
    private Logger logger = Logger.getLogger("steeldb_replica");
    
    int count = 0;
    
    public Replica(int id, Properties config) {
        
		super();
		connParams = new ConnectionParams(config);
		mProcessor = new MessageProcessor(id, connParams.getDriver(), connParams.getUrl());
                this.id = id;
		replica = new ServiceReplica(id,this,this);                
                View view = replicaContext.getCurrentView();
		mProcessor.setCurrentView(view);
		logger.info("Replica started");
	}
        
        @Override
        public byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtxs) {


            byte [][] replies = new byte[commands.length][];
            for (int i = 0; i < commands.length; i++) {
                if(msgCtxs != null && msgCtxs[i] != null) {
                replies[i] = executeSingle(commands[i],msgCtxs[i]);
                }
                else executeSingle(commands[i],null);
            }

            return replies;
        }
                
	private synchronized byte[] executeSingle(byte[] command, MessageContext msgCtx) {
		byte [] reply = null;
		
		Message m = Message.getMessage(command);
		Message replyMsg = null;
		
		logger.debug("ordered, " + m.getClientId() + ", " + m.getContents());
		
		switch(m.getOpcode()) {
		case OpcodeList.GET_STATE:
			SMMessage smmsg = (SMMessage)m.getContents();
			if(smmsg.getSender() != id)
				stateManager.SMRequestDeliver(smmsg, true);
			replyMsg = new Message(OpcodeList.STATE_REQUESTED, null, false);
			break;
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
		
        return reply;
	}

	@Override
	public synchronized byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
		byte[] replyBytes = null;
		Message reply = null;
		Message m = Message.getMessage(command);

		logger.debug("unordered, " + m.getClientId() + ", " + m.getContents());

		switch(m.getOpcode()) {
		case OpcodeList.EXECUTE:
			reply = mProcessor.processExecute(m, m.getClientId());
		case OpcodeList.SET_FETCH_SIZE:
			break;
		case OpcodeList.EXECUTE_BATCH:
			reply = mProcessor.processExecuteBatch(m, m.getClientId());
			break;
		case OpcodeList.EXECUTE_QUERY:
			reply = mProcessor.processExecuteQuery(m, m.getClientId());
			break;
		case OpcodeList.EXECUTE_UPDATE:
			reply = mProcessor.processExecuteUpdate(m, m.getClientId());
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
		return replyBytes;
	}

	// Method used for TClouds demo
	public byte[] getSnapshotH2() {
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
	}

	@Override
	public byte[] getSnapshot() { // To run with Postgres. It does not take the dump
		byte[] dump = new byte[0];
		logger.debug("Dump taken. Size: " + dump.length + " bytes.");
		List<DBConnectionParams> connParams = mProcessor.getConnections();
		DatabaseState dbState = new DatabaseState(dump, null, connParams, mProcessor.getMaster());
		return TOMUtil.getBytes(dbState);
	}
	
	@Override
	public void installSnapshot(byte[] snapshot) {
		StringBuffer dropStatements = new StringBuffer();
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
		logger.debug("--- OPEN");
	}

	@Override
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
	}

}
