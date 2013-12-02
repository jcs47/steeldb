package lasige.steeldb.test;

import java.io.Console;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;


public class StateTransferTest {

    private Logger logger = Logger.getLogger("steeldb_client");

    private static String connectionURL = "jdbc:bftdriver;foo0;foo1;foo2;foo3";
	private static String username = "foo;foo;foo;foo";
	private static String password = "bar;bar;bar;bar";
	private Connection conn;
	private Console console;
	
	public static void main(String[] args) {
		StateTransferTest mct = new StateTransferTest();
		mct.console = System.console();
		mct.testLogin();
		mct.testCleanTable();
		mct.testInsertUser();
//		mct.testBFTSMaRtLeaderChange();
		mct.testStateTransfer();
		mct.testLogout();
	}
	

	public void testLogin() {
		try {
			Class.forName("lasige.steeldb.jdbc.BFTDriver");
			logger.info("Connecting to Foo database");
			conn = DriverManager.getConnection(connectionURL, username, password);
			logger.info("Connected");
			conn.setAutoCommit(true);
			logger.info("AutoCommit set to true");

			logger.info("SELECT COUNT(USERID) FROM USER");
			PreparedStatement pst = conn.prepareStatement("SELECT COUNT(USERID) FROM USER");
			ResultSet rs = pst.executeQuery();
			if(rs.next())
				logger.info("COUNT: " + rs.getInt(1));
			else
				logger.info("Problem selecting from USER");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void testLogout() {
		try {
			logger.info("Closing the connection");
			conn.close();
			logger.info("Conection closed: " + conn.isClosed());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void testCleanTable() {
		try {
			logger.info("Testing clean table: DELETE FROM USER WHERE 1");
			PreparedStatement pst = conn.prepareStatement("DELETE FROM USER WHERE 1");
			int rowsAffected = pst.executeUpdate();
			logger.info("Rows affected: " + rowsAffected);
 		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void testInsertUser() {
		try {
			logger.info("Testing insert user");
			PreparedStatement pst = conn.prepareStatement("INSERT INTO USER(NAME) VALUES(?)");
			pst.setString(1, "TClouds user number 1");
			int rowsAffected = pst.executeUpdate();
			logger.info("Rows affected: " + rowsAffected);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Before execute this step it is necessary to manually stop the master replica.
	 */
	public void testBFTSMaRtLeaderChange() {
		try {
			logger.info("Testing BFT-SMaRt leader change");
			console.readLine("Stop BFT-SMaRt leader replica and press enter");
			logger.info("BFT-SMaRt leader replica is supposed to be stopped. Will proceed with the test execution.");
			PreparedStatement pst = conn.prepareStatement("INSERT INTO USER(NAME) VALUES(?)");
			pst.setString(1, "TClouds user number 2");
			logger.info("Inserting into USER table.");
			int rowsAffected = pst.executeUpdate();
			logger.info("Test passed. Rows affected: " + rowsAffected);
		} catch (SQLException e) {
			logger.error("Error testing BFT-SMaRt leader change", e);
		}
	}

	/**
	 * Before execute this step it is necessary to manually stop the master replica.
	 */
	public void testStateTransfer() {
		try {
			logger.info("Testing state transfer change");
			conn.setAutoCommit(false);
			logger.info("Autocommit set to false");
			logger.info("SELECT COUNT(USERID) FROM USER");
			PreparedStatement pstSelect = conn.prepareStatement("SELECT COUNT(USERID) FROM USER");
			ResultSet rs = pstSelect.executeQuery();
			if(rs.next())
				logger.info("COUNT: " + rs.getInt(1));
			else
				logger.info("Problem selecting from USER");
			logger.info("Executing the first write operation");
			
			PreparedStatement pst = conn.prepareStatement("INSERT INTO USER(NAME) VALUES(?)");
			for(int i = 2; i < 7; i++) {
				pst.setString(1, "TClouds user number " + i);
				int rowsAffected = pst.executeUpdate();
				logger.info("Rows affected: " + rowsAffected);
				rs = pstSelect.executeQuery();
				if(rs.next())
					logger.info("COUNT: " + rs.getInt(1));
			}
			rs = pstSelect.executeQuery();
			if(rs.next()) {
				logger.info("COUNT: " + rs.getInt(1));
				logger.info("Select count worked. Selected user after replica change");
			}
			pst.setString(1, "TClouds user number 7");
			pst.executeUpdate();
			conn.commit();
			conn.setAutoCommit(true);
			logger.info("Transaction committed. Autocommit set to true.");
			
			console.readLine("Stop a non master replica and press enter");

			pst.setString(1, "TClouds user number 8");
			pst.executeUpdate();

			pst.setString(1, "TClouds user number 9");
			pst.executeUpdate();
			logger.info("User 8 and 9 inserted.");

			console.readLine("Start the replica again and press enter");

			pst.setString(1, "TClouds user number 10");
			pst.executeUpdate();

			pst.setString(1, "TClouds user number 11");
			pst.executeUpdate();
			logger.info("User 10 and 11 inserted.");

			console.readLine("Stop another non master replica and press enter");

			pstSelect = conn.prepareStatement("SELECT * FROM USER");
			rs = pstSelect.executeQuery();
			logger.info("Result from select");
			while(rs.next()) {
				logger.info("user: " + rs.getInt(1) + ", name: " + rs.getString(2));
			}
			
			logger.info("Test finished. State transfer worked");
			
			
			
			logger.info("Transaction commited");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}