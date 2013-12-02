package lasige.steeldb.Replica;

import java.util.Properties;

class ConnectionParams {

	private final String driver;
	private final String url;
	private final String database;
	private final String user;
	private final String password;
	
	ConnectionParams(Properties config) {
		this.driver = config.getProperty("driver");
		this.url = config.getProperty("url");
		this.database = config.getProperty("database");
		this.user = config.getProperty("user");
		this.password = config.getProperty("password");;
	}

	protected String getDriver() {
		return driver;
	}

	protected String getUrl() {
		return url;
	}

	protected String getDatabase() {
		return database;
	}

	protected String getUser() {
		return user;
	}

	protected String getPassword() {
		return password;
	}
	
}
