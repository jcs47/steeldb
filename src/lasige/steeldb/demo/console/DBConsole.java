package lasige.steeldb.demo.console;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

public class DBConsole {
	
	private static final String connectionURL = "jdbc:bftdriver;booktown0;booktown1;booktown2;booktown3";
	private static final String username = "postgres;postgres;postgres;postgres";
	private static final String password = "rammstein;rammstein;rammstein;rammstein";

        public static void main(String[] args) {
		try {
			Class.forName("lasige.steeldb.jdbc.BFTDriver");
			Connection conn = DriverManager.getConnection(connectionURL, username, password);
			conn.setAutoCommit(false);
			Scanner in = new Scanner(System.in);
                        System.out.println("Connect. Type your commands. \n\n");
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
				} else if(line.toLowerCase().startsWith("update") || line.toLowerCase().startsWith("insert")) {
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
