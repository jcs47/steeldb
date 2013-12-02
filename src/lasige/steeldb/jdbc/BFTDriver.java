package lasige.steeldb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import lasige.steeldb.comm.LoginRequest;
import lasige.steeldb.comm.Message;
import lasige.steeldb.comm.MessageHandler;
import lasige.steeldb.comm.OpcodeList;


public class BFTDriver implements java.sql.Driver {

	public final static Pattern QUERY_PATTERN = Pattern.compile("^SELECT .*", Pattern.DOTALL|Pattern.CASE_INSENSITIVE);
	public final static Pattern UPDATE_PATTERN = Pattern.compile("^(INSERT|DELETE|UPDATE) .*", Pattern.DOTALL|Pattern.CASE_INSENSITIVE);

	private static final int MAJOR_VERSION = 1;
	private static final int MINOR_VERSION = 0;
	private static final AtomicInteger clientId = new AtomicInteger(0);

    private Logger logger = Logger.getLogger("steeldb_client");

    static {
		BFTDriver driver = new BFTDriver();
		try {
			DriverManager.registerDriver(driver);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith("jdbc:bftdriver");
	}

	@Override
	public Connection connect(String url, Properties props) throws SQLException {
		String[] urlData = url.split(";");
		if(!urlData[0].equalsIgnoreCase("jdbc:bftdriver"))
			return null;
		int replicaCount = urlData.length - 1;
		Vector<String> databases = new Vector<String>(replicaCount);
		for(int i = 1; i < urlData.length; i++) {
			databases.add(urlData[i]);
		}
		String[] userList = props.getProperty("user").split(";");
		String[] passList = props.getProperty("password").split(";");
		if(userList.length != replicaCount || passList.length != replicaCount)
			throw new SQLException("Invalid username or password");
		Vector<String> users = new Vector<String>(replicaCount);
		Vector<String> passwords = new Vector<String>(replicaCount);
		for(int i = 0; i < replicaCount; i++) {
			users.add(userList[i]);
			passwords.add(passList[i]);
		}
		LoginRequest lr = new LoginRequest(databases,users,passwords);
		
		//create mHandler
		MessageHandler mHandler =  new MessageHandler(clientId.incrementAndGet());
		//do the login proccess
		Message login = new Message(OpcodeList.LOGIN_SEND, lr, false);
		Message reply = mHandler.send(login, true);
		if(reply.getOpcode() != OpcodeList.LOGIN_OK)
			throw new SQLException("Error establishing connection:"
					+ "Invalid username or password");
		BFTConnection connection = new BFTConnection(mHandler);
		connection.setClientInfo("ClientUser", String.valueOf(clientId));
		//move to next clientId to be used
		return connection;
	}

	@Override
	public int getMajorVersion() {
		return MAJOR_VERSION;
	}

	@Override
	public int getMinorVersion() {
		return MINOR_VERSION;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties props)
			throws SQLException {
		return new DriverPropertyInfo[0];
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}
}
