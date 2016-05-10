package lasige.steeldb.jdbc;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

import lasige.steeldb.comm.Message;
import lasige.steeldb.comm.MessageHandler;
import lasige.steeldb.comm.OpcodeList;

public class BFTStatement implements java.sql.Statement {

	private MessageHandler mHandler;
	private ResultSet generatedKeys;
	private Connection connection;

	protected int maxRows = -1;
	protected int fetchSize = -1;

	public BFTStatement(MessageHandler mHandler, Connection connection) {
        super();
        this.mHandler = mHandler;
        this.connection = connection;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
    	Message m = new Message(OpcodeList.EXECUTE, sql, true, mHandler.getMaster());
		Message reply = mHandler.send(m, this.getConnection().getAutoCommit());
		if(reply.getOpcode() == OpcodeList.EXECUTE_ERROR)
			throw new SQLException("Error executing sql");
		else if(reply.getOpcode() == OpcodeList.TIMEOUT)
			throw new SQLException("Timeout during execution");
		else if(reply.getOpcode() == OpcodeList.ABORTED)
			throw new SQLException("Transaction aborted");
		else if(reply.getOpcode() == OpcodeList.MASTER_CHANGE_ERROR)
			throw new SQLException("Master change request error");
		return (Integer)reply.getContents() == 1;
	}

	@Override
    public ResultSet executeQuery(String query) throws SQLException {
		Message m = new Message(OpcodeList.EXECUTE_QUERY, query, true, mHandler.getMaster());
		Message reply = null;
		if(this.getConnection().getAutoCommit())
			reply = mHandler.send(m, true);
		else
			reply = mHandler.send(m, false);
		ResultSet rs = (ResultSet) reply.getContents();
		return rs;
    }

    @Override
    public int executeUpdate(String query) throws SQLException {
		Message m = new Message(OpcodeList.EXECUTE_QUERY, query, true, mHandler.getMaster());
		Message reply = null;
		if(this.getConnection().getAutoCommit())
			reply = mHandler.send(m, false);
		else
			reply = mHandler.send(m, true);
		int result = -1;
		if(reply.getContents() instanceof List) {
			List replyList = (List)reply.getContents();
			result = (Integer)replyList.get(0);
			setGeneratedKeys((BFTRowSet)replyList.get(1));
			result = 1;
		} else
			result = (Integer)reply.getContents();
		return result;
    }

    
    @Override
	public void setFetchSize(int val) throws SQLException {
//		Message m = new Message(OpcodeList.SET_FETCH_SIZE, val, false);
//		mHandler.send(m, this.getConnection().getAutoCommit());
    	this.fetchSize = val;
	}

	@Override
	public void setMaxRows(int val) throws SQLException {
//		Message m = new Message(OpcodeList.SET_MAX_ROWS, val, false);
//		mHandler.send(m, this.getConnection().getAutoCommit());
		this.maxRows = val;
	}

	@Override
    public void close() throws SQLException {}

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBatch(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancel() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String arg0, int arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String arg0, int[] arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String arg0, String[] arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String arg0, int arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
	public int getMaxRows() throws SQLException {
    	return this.maxRows;
	}

	@Override
    public Connection getConnection() throws SQLException {
		return connection;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFetchSize() throws SQLException {
    	return this.fetchSize;
    }

	public void setGeneratedKeys(ResultSet generatedKeys) {
		this.generatedKeys = generatedKeys;
	}

	@Override
    public ResultSet getGeneratedKeys() throws SQLException {
    	return generatedKeys;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getMoreResults(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCursorName(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setEscapeProcessing(boolean arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFetchDirection(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMaxFieldSize(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPoolable(boolean arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setQueryTimeout(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

	@Override
	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

}
