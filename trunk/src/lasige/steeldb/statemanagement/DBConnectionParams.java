package lasige.steeldb.statemanagement;

import java.io.Serializable;

/**
 * 
 * @author mhsantos
 *
 */
public class DBConnectionParams implements Serializable {

	private static final long serialVersionUID = -8067097559979424348L;
	private int clientId;
	private final String database;
	private final String user;
	private final String password;
	private volatile int hashCode;
	
	public DBConnectionParams(String database, String user, String password) {
		this(-1, database, user, password);
	}

	public DBConnectionParams(int clientId, String database, String user, String password) {
		this.clientId = clientId;
		this.database = database;
		this.user = user;
		this.password = password;
	}
	
	public void setClientId(int clientId) {
		this.clientId = clientId;
	}

	public int getClientId() {
		return clientId;
	}
	
	public String getDatabase() {
		return database;
	}
	public String getUser() {
		return user;
	}
	public String getPassword() {
		return password;
	}
	
	@Override
	public boolean equals(Object o) {
		if(o == this)
			return true;
		if(!(o instanceof DBConnectionParams))
			return false;
		DBConnectionParams dblp = (DBConnectionParams)o;
		if(!this.equals(dblp.database))
			return false;
		if(!this.equals(dblp.user))
			return false;
		if(!this.equals(dblp.password))
			return false;
		return true;
	}
	
	@Override
	public int hashCode() {
		int result = hashCode;
		if(result == 0) {
			result = 17;
			result = 31 * result + user.hashCode();
			result = 31 * result + password.hashCode();
		}
		return result;
	}
}
