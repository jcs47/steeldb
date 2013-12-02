package lasige.steeldb.demo.console;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

public class DBConsole {
	
	private static String connectionURL = "jdbc:bftdriver;smartlighting0;smartlighting1;smartlighting2;smartlighting3";
	private static String username = "sl;sl;sl;sl";
	private static String password = "sl;sl;sl;sl";

	public static void main(String[] args) {
		try {
			Class.forName("lasige.steeldb.jdbc.BFTDriver");
			Connection conn = DriverManager.getConnection(connectionURL, username, password);
			conn.setAutoCommit(false);
			Scanner in = new Scanner(System.in);
			String line = in.nextLine();
			while(!(line.equalsIgnoreCase("exit"))) {
				if(line.toLowerCase().startsWith("select")) {
					PreparedStatement pst = conn.prepareStatement(line);
					ResultSet rs = pst.executeQuery();
					int count = 0;
					while(rs.next()) {
						count++;
					}
					System.out.println("Query executed. Results: " + count);
				} else if(line.toLowerCase().startsWith("update")) {
					PreparedStatement pst = conn.prepareStatement(line);
					int affectedRows = pst.executeUpdate();
					System.out.println("Update executed. Affected rows: " + affectedRows);
				} else if(line.toLowerCase().startsWith("commit")) {
					conn.commit();
					System.out.println("commited");
				} else if(line.toLowerCase().startsWith("rollback")) {
					conn.rollback();
					System.out.println("rolled back");
				}
				line = in.nextLine();
			}
			System.out.println("exiting");
			in.close();
			conn.close();
			System.exit(0);
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
