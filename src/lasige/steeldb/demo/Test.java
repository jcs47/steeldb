/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lasige.steeldb.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lasige.steeldb.jdbc.ResultSetData;
import merkletree.TreeCertificate;
import static merkletree.TreeCertificate.jsonToLeafs;
import static merkletree.TreeCertificate.makeMerkleTree;
import org.json.JSONArray;

/**
 *
 * @author snake
 */
public class Test {
        // JDBC driver name and database URL
        static final String JDBC_DRIVER = "org.postgresql.Driver";
        static final String DB_URL = "jdbc:postgresql://localhost:5432/rubis";

        //  Database credentials
        static final String USER = "benchmarksql";
        static final String PASS = "password";
        
        public static void main(String[] args) {
            System.out.println("Test next power of 2: " + TreeCertificate.nextPowerOf2(200));

            Connection conn = null;
            PreparedStatement stmt = null;
            try {
                //STEP 2: Register JDBC driver
                Class.forName(JDBC_DRIVER);

                //STEP 3: Open a connection
                System.out.println("Connecting to database...");
                conn = DriverManager.getConnection(DB_URL, USER, PASS);

                //STEP 4: Execute a query
                System.out.println("Creating statement...");
                
                String sql;
                sql = "SELECT items.name, items.id, items.end_date, items.max_bid, items.nb_of_bids, items.initial_price FROM items WHERE items.category=? AND end_date>=NOW() ORDER BY items.end_date ASC LIMIT ? OFFSET ?";
                stmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                
                stmt.setInt(1, 1);
                stmt.setInt(2, 10);
                stmt.setInt(3, 1 * 10);
      
                ResultSet rs = stmt.executeQuery();

                //STEP 5: Extract data from result set
                while (rs.next()) {
                    //Retrieve by column name
                    String itemName = rs.getString("name");
                    int itemId = rs.getInt("id");
                    String endDate = rs.getString("end_date");
                    float maxBid = rs.getFloat("max_bid");
                    int nbOfBids = rs.getInt("nb_of_bids");
                    float initialPrice = rs.getFloat("initial_price");
                    if (maxBid < initialPrice)
                          maxBid = initialPrice;

                    //Display values
                    System.out.print("name: " + itemName);
                    System.out.print(", id: " + itemId);
                    System.out.print(", end date: " + endDate);
                    System.out.print(", max bid: " + maxBid);
                    System.out.print(", number of bid: " + nbOfBids);
                    System.out.println(", initial price: " + initialPrice);                    

                }
                rs.beforeFirst();
                
                ResultSetData rsd = new ResultSetData(rs);
                JSONArray json = TreeCertificate.getJSON(rsd.getRows());
                System.out.println("JSON format: " + json);
                System.out.println("MerkleTree: ");

                System.out.println(makeMerkleTree(jsonToLeafs(json)));

                sql = "SELECT * FROM items WHERE id=?";
                stmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                stmt.setInt(1, 2017);
                
                rs = stmt.executeQuery();
                rsd = new ResultSetData(rs);
                
                json = TreeCertificate.getJSON(rsd.getRows());
                System.out.println("JSON format: " + json);
                System.out.println("MerkleTree: ");

                System.out.println(makeMerkleTree(jsonToLeafs(json)));
                
                //STEP 6: Clean-up environment
                rs.close();
                stmt.close();
                conn.close();
            } catch (SQLException se) {
                //Handle errors for JDBC
                se.printStackTrace();
            } catch (Exception e) {
                //Handle errors for Class.forName
                e.printStackTrace();
            } finally {
                //finally block used to close resources
                try {
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException se2) {
                }// nothing we can do
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException se) {
                    se.printStackTrace();
                }//end finally try
            }//end try
            System.out.println("Goodbye!");
        }
    
}
