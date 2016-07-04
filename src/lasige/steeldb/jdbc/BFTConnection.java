package lasige.steeldb.jdbc;


import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection; 
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import lasige.steeldb.comm.Message;
import lasige.steeldb.comm.MessageHandler;
import lasige.steeldb.comm.OpcodeList;

public class BFTConnection implements Connection {

	private MessageHandler mHandler;
	private boolean autoCommit = true;
	private boolean closed = true;
	
	private Properties clientInfo = new Properties();
	
	public BFTConnection(MessageHandler mHandler) {
		super();
		this.mHandler = mHandler;
		this.closed = false;
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		PreparedStatement pstmt = new BFTPreparedStatement(this, mHandler, sql); 
		return pstmt;
	}

	@Override
	public void close() throws SQLException {
		Message close = new Message(OpcodeList.CLOSE,null,false, mHandler.getMaster());
		Message reply = mHandler.send(close, true);
		if(reply.getOpcode() == OpcodeList.CLOSE_ERROR)
			throw new SQLException("BFTConnection.close() exception");
		mHandler.close();
		closed = true;
	}

	@Override
	public Statement createStatement() throws SQLException {
		BFTStatement stmt = new BFTStatement(mHandler, this);
		return stmt;
		
	}

	@Override 
	public void rollback() throws SQLException {
		Message rollback = new Message(OpcodeList.ROLLBACK_SEND, null, false, mHandler.getMaster()); //readonly?
		Message reply = mHandler.send(rollback, false);
		if(reply.getOpcode() == OpcodeList.ROLLBACK_ERROR)
                    if ((reply.getContents() == null) || !(reply.getContents() instanceof SQLException))
			throw new SQLException("Error during rollback");
                else
                        throw (SQLException) reply.getContents();
	}

	@Override
	public void commit() throws SQLException {
            
                if (autoCommit || closed || !mHandler.hasOps()) return;
            
		LinkedList<Integer> hashTable = new LinkedList<Integer>();
		Message commit = new Message(OpcodeList.COMMIT, hashTable, false, mHandler.getMaster()); //readonly?
		Message reply = mHandler.send(commit, false);
		
		if(reply.getOpcode() == OpcodeList.COMMIT_ERROR) {
                     if ((reply.getContents() == null) || !(reply.getContents() instanceof SQLException))
			throw new SQLException("Commit Error");
                    else
                        throw (SQLException) reply.getContents();
                } else if(reply.getOpcode() == OpcodeList.TIMEOUT) {
			throw new SQLException("Timeout during commit");
		} else if(reply.getOpcode() == OpcodeList.ABORTED) {
                     if ((reply.getContents() == null) || !(reply.getContents() instanceof SQLException))
                        throw new SQLException("Transaction aborted");
                    else
                        throw (SQLException) reply.getContents();
                }

	}
	
	@Override
	public void setAutoCommit(boolean flag) throws SQLException {
		if(flag != autoCommit) {
			Message autoCommitMessage = new Message(OpcodeList.COMMIT_AUTO, flag, false, mHandler.getMaster()); //readonly?
			Message reply = mHandler.send(autoCommitMessage, true);
			if(reply.getOpcode() == OpcodeList.COMMIT_AUTO_ERROR)
				throw new SQLException("Error setting auto-commit flag");
			else if(reply.getOpcode() == OpcodeList.TIMEOUT)
				throw new SQLException("Timeout setting auto-commit flag");
			autoCommit = flag;
		}
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Statement createStatement(int arg0, int arg1) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Statement createStatement(int arg0, int arg1, int arg2)
	throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return autoCommit;
	}

	@Override
	public String getCatalog() throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return clientInfo;
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return clientInfo.getProperty(name);
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	/* TODO: Remove the connection code and load searialized metadata
	 * informacion according to the database defined
	 * @see java.sql.Connection#getMetaData()
	 */
	public DatabaseMetaData getMetaData() throws SQLException {
		Integer clientId = Integer.valueOf(this.getClientInfo("ClientUser"));
		Message metadataMessage = new Message(OpcodeList.GET_DB_METADATA, clientId, false, mHandler.getMaster()); //readonly?
		Message reply = mHandler.send(metadataMessage, true);
		BFTDatabaseMetaData bftDBM = null;
		if(reply.getOpcode() == OpcodeList.GET_DB_METADATA_OK)
			bftDBM = (BFTDatabaseMetaData)reply.getContents();
		return bftDBM;
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isValid(int arg0) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String nativeSQL(String arg0) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CallableStatement prepareCall(String arg0) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2)
	throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
	throws SQLException {
		return new BFTPreparedStatement(this, mHandler, sql, autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
	throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
	throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
                            int resultSetType, int resultSetConcurrency)
	throws SQLException {
		return new BFTPreparedStatement(this, mHandler, sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
                            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void releaseSavepoint(Savepoint arg0) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void rollback(Savepoint arg0) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCatalog(String arg0) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setClientInfo(Properties arg0) throws SQLClientInfoException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setClientInfo(String name, String value)
	throws SQLClientInfoException {
		clientInfo.setProperty(name, value);
	}

	@Override
	public void setHoldability(int arg0) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setReadOnly(boolean arg0) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Savepoint setSavepoint(String arg0) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTransactionIsolation(int arg0) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getSchema() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

}
