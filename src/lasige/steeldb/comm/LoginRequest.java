package lasige.steeldb.comm;

import java.io.Serializable;
import java.util.Vector;

public class LoginRequest implements Serializable{
	
	private static final long serialVersionUID = 4358847389970721117L;
	
	private final Vector<String> users;
	private final Vector<String> passwords;
	private final Vector<String> databases;
	
	public LoginRequest(Vector<String> databases, Vector<String> users,
			Vector<String> passwords) {
		this.databases = databases;
		this.users = users;
		this.passwords = passwords;
	}

	public String getDatabase(int replicaId) {
		return databases.get(replicaId);
	}

	public String getUser(int replicaId) {
		return users.get(replicaId);
	}

	public String getPassword(int replicaId) {
		return passwords.get(replicaId);
	}
	
}
