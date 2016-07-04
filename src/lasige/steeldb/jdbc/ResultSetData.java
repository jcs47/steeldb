package lasige.steeldb.jdbc;

import java.io.Serializable;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.Struct;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.serial.SerialArray;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialException;
import javax.sql.rowset.serial.SerialStruct;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import bftsmart.tom.util.TOMUtil;
import java.util.LinkedList;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import merkletree.Leaf;
import merkletree.MerkleTree;
import org.json.JSONArray;

/**
 * 
 * @author mhsantos
 *
 *
 */

public class ResultSetData implements Serializable {
	
	private static final long serialVersionUID = 4017257435190384824L;

	private final RowSetMetaDataImpl metadata;
	private final LinkedList<LinkedList<Object>> rows;
	private boolean updateOnInsert;
	private int numCols;

    private static Logger logger = Logger.getLogger("steeldb_client");

	public RowSetMetaDataImpl getMetadata() {
		return metadata;
	}

	public LinkedList<LinkedList<Object>> getRows() {
		return rows;
	}

	public boolean isUpdateOnInsert() {
		return updateOnInsert;
	}

	public ResultSetData(ResultSet rs) {
		RowSetMetaDataImpl mdImpl = new RowSetMetaDataImpl();
		try {
			initMetaData(mdImpl, rs.getMetaData());
		} catch (SQLException e) {
			logger.error("error initializing meta data");
		}
		metadata = mdImpl;
		rows = populate(rs);
	}

	private void initMetaData(RowSetMetaDataImpl md, ResultSetMetaData rsmd) {
		try {
			numCols = rsmd.getColumnCount();
			md.setColumnCount(numCols);
//			logger.debug("---[initial mestadata: " + Arrays.toString(TOMUtil.computeHash(TOMUtil.getBytes(md))));
			for (int col=1; col <= numCols; col++) {
				md.setAutoIncrement(col, rsmd.isAutoIncrement(col));
				if(rsmd.isAutoIncrement(col))
					updateOnInsert = true;
				md.setCaseSensitive(col, false);
				md.setCurrency(col, false);
				md.setNullable(col, rsmd.isNullable(col));
				md.setSigned(col, rsmd.isSigned(col));
				md.setSearchable(col, false);
				/*
				 * The PostgreSQL drivers sometimes return negative columnDisplaySize,
				 * which causes an exception to be thrown.  Check for it.
				 */
				int size = rsmd.getColumnDisplaySize(col);
				if (size < 0) {
					size = 0;
				}
				md.setColumnDisplaySize(col, 0);
	
				if (StringUtils.isNotBlank(rsmd.getColumnLabel(col))) {
					md.setColumnLabel(col, rsmd.getColumnLabel(col).toLowerCase());
				}
	
				if (StringUtils.isNotBlank(rsmd.getColumnName(col))) {
					md.setColumnName(col, rsmd.getColumnName(col).toLowerCase());
				}
	
				md.setSchemaName(col, null);
				/*
				 * Drivers return some strange values for precision, for non-numeric data, including reports of
				 * non-integer values; maybe we should check type, & set to 0 for non-numeric types.
				 */
				int precision = rsmd.getPrecision(col);
				if (precision < 0) {
					precision = 0;
				}
				md.setPrecision(col, 0);
	
				/*
				 * It seems, from a bug report, that a driver can sometimes return a negative
				 * value for scale.  javax.sql.rowset.RowSetMetaDataImpl will throw an exception
				 * if we attempt to set a negative value.  As such, we'll check for this case.
				 */
				int scale = rsmd.getScale(col);
				if (scale < 0) {
					scale = 0;
				}
				md.setScale(col, 0);
				md.setTableName(col, null);
				md.setCatalogName(col, null);
				md.setColumnType(col, -1);
				md.setColumnTypeName(col, null);
//				logger.debug("---[after iteration " + col + ": " + Arrays.toString(TOMUtil.computeHash(TOMUtil.getBytes(md))) + ", tpe:" + rsmd.getColumnType(col) + ", tpenm:" + rsmd.getColumnTypeName(col));
			}
		} catch (SQLException e) {
			logger.error("error inside initMetaData", e);
		}

	}
	
	private LinkedList<LinkedList<Object>> populate(ResultSet data) {
		LinkedList<LinkedList<Object>> rows = new LinkedList<LinkedList<Object>>();
		int countRow = 0;
		try {
			while (data.next()) {
				LinkedList<Object> cols = new LinkedList<>();
				int countCol = 1;
				for (int i = 1; i <= numCols; i++) {
					Object obj = data.getObject(i);
					String objtype = "";
					if (obj instanceof Struct) {
						obj = new SerialStruct((Struct)obj, null);
						objtype = "serialstruct";
					} else if (obj instanceof SQLData) {
						obj = new SerialStruct((SQLData)obj, null);
						objtype = "sqldata";
					} else if (obj instanceof Blob) {
						obj = new SerialBlob((Blob)obj);
						objtype = "blob";
					} else if (obj instanceof Clob) {
						obj = new SerialClob((Clob)obj);
						objtype = "clob";
					} else if (obj instanceof java.sql.Array) {
						obj = new SerialArray((java.sql.Array)obj, null);
						objtype = "array";
					} else if (obj instanceof java.lang.Integer) {
						obj = new Integer((Integer)obj);
						objtype = "integer";
					} else if (obj instanceof java.lang.Double) {
						obj = new Double((Double)obj);
						objtype = "double";
					} else if (obj instanceof java.lang.String) {
						obj = new String((String)obj);
						objtype = "string";
					} else if (obj instanceof java.util.Date) {
						objtype = "utildate";
						java.util.Date dataobj = (java.util.Date)obj;
						long millis = dataobj.getTime();
						SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss.SSS");
//						logger.debug("millis: " + millis + ", time: " + sdf.format(dataobj));
					} else if (obj instanceof java.sql.Date) {
						objtype = "sqldte";
					} else if (obj instanceof java.sql.Time) {
						objtype = "sqltime";
					} else if (obj instanceof java.sql.Timestamp) {
						objtype = "sqltimestamp";
					}
					cols.add(obj);
//					logger.debug("---[row " + countRow + ", col " + countCol + " : " + Arrays.toString(TOMUtil.computeHash(TOMUtil.getBytes(obj))) + ", obj: " + obj + ", type: " + objtype);
					countCol++;
				}
				rows.add(cols);
				countRow++;
			}
		} catch (SerialException e) {
			logger.error("serial error populating", e);
		} catch (SQLException e) {
			logger.error("sqlexception populating", e);
		}
		return rows;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof ResultSetData))
			return false;
		ResultSetData other = (ResultSetData)obj;
		if (rows == null) {
			if (other.rows != null)
				return false;
		} else if (rows.size() != other.rows.size() || !rows.containsAll(other.rows))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if(rows != null) {
			for(LinkedList<Object> row : rows) {
				sb.append("[row:");
				for(Object col : row) {
					sb.append(col).append(";");
				}
				sb.append("]\n");
			}
		}
                else {
                    sb.append("[empty]");
                }
//		sb.append("metadata: ");
//		
//		int colCount;
//		try {
//			colCount = metadata.getColumnCount();
//			sb.append("colcount:").append(colCount).append(",");
//			sb.append("class:").append(metadata.getClass());
//			for(int i = 1; i <= colCount; i++) {
//				sb.append("[");
//				sb.append(metadata.isAutoIncrement(i)).append(",");
//				sb.append(metadata.isCaseSensitive(i)).append(",");
//				sb.append(metadata.isCurrency(i)).append(",");
//				sb.append(metadata.isNullable(i)).append(":").append(Arrays.toString(TOMUtil.computeHash(TOMUtil.getBytes(metadata.isNullable(i))))).append(",");
//				sb.append(metadata.isSigned(i)).append(",");
//				sb.append(metadata.isSearchable(i)).append(",");
//				sb.append(metadata.getColumnDisplaySize(i)).append(",");
//				sb.append(metadata.getColumnLabel(i)).append(",");
//				sb.append(metadata.getColumnName(i)).append(",");
//				sb.append(metadata.getSchemaName(i)).append(",");
//				sb.append(metadata.getPrecision(i)).append(",");
//				sb.append(metadata.getScale(i)).append(",");
//				sb.append(metadata.getTableName(i)).append(",");
//				sb.append(metadata.getCatalogName(i)).append(",");
//				sb.append(metadata.getColumnType(i)).append(",");
//				sb.append(metadata.getColumnTypeName(i)).append(",");
//				sb.append(metadata.isReadOnly(i)).append(",");
//				sb.append(metadata.isWritable(i)).append(",");
//			}
//		} catch (SQLException e) {
//			logger.error("error iterating over metadata", e);
//		}
//		sb.append("]");
		return sb.toString();
	}

        public JSONArray getJSON() {
            JSONArray result = new JSONArray();
            
            for (int i = 0; i < rows.size(); i++) {
                
                
                JSONArray row = new JSONArray();
                
                for (int j = 0; j < rows.get(i).size(); j++) {
                    
                    row.put(j, rows.get(i).get(j).toString());
                }
                
                result.put(i, row);
                
            }
            
            return result;
        }
        
        public static Leaf[] jsonToLeafs(JSONArray rs) {
            
            if (rs == null) return null;
            
            int nRows = 2;
                        
            if (rs.length() > 1) {
                nRows = nextPowerOf2(rs.length()); // this ensures that the number of data blocks is a power of 2
            }
            
            // Create data blocks to be assigned to leaf nodes
            Leaf[] leafs = new Leaf[nRows];
                       
            for (int i = 0; i < nRows; i++) {
                
                List<byte[]> l = new LinkedList<>();

                if (i < rs.length()) {
                    
                    JSONArray col = (JSONArray) rs.get(i);
                    
                    for (int j = 0; j < col.length(); j++) {
                        l.add(col.get(j).toString().getBytes(StandardCharsets.UTF_8));
                    }
                    
                } else {
                    l.add(new byte[0]);
                }
                
                leafs[i] = new Leaf(l);
            }
            
            return leafs;
        }
        
        public static MerkleTree makeMerkleTree(Leaf[] leafs) {
            boolean inLeafs = true;
            MerkleTree[] branches = new MerkleTree[leafs.length];

            // Define the message digest algorithm to use
            MessageDigest md = null;
            try 
            {
                    md = MessageDigest.getInstance("SHA");
            } 
            catch (NoSuchAlgorithmException e) 
            {
                    // Should never happen, we specified SHA, a valid algorithm
                    assert false;
            }            
            
            do {
                
                MerkleTree[] m = new MerkleTree[branches.length/2];
                
                
                if (inLeafs) {
                    
                    for (int i = 0, j = 0; i < leafs.length; i += 2, j++) {                        
                        
                        m[j] = new MerkleTree(md);
                        m[j].add(leafs[i], leafs[i+1]);
                    }
                    
                    inLeafs = false;
                    
                } else {
                    for (int i = 0, j = 0; i < branches.length; i += 2, j++) {
                        m[j] = new MerkleTree(md);
                        m[j].add(branches[i], branches[i+1]);
                    }
                }
                
                branches = m;
                
            } while(branches.length > 1);
            
                        
            return branches[0];
            
        }
        
        public MerkleTree getMerkleTree() {
            
            return makeMerkleTree(jsonToLeafs(getJSON()));
            
        }        
        
        
        private static int nextPowerOf2(int n) {
            int p = (n == 0 ? 0 : 32 - Integer.numberOfLeadingZeros(n - 1));
            return (int) Math.pow(2, p);
        }        
                
        // JDBC driver name and database URL
        static final String JDBC_DRIVER = "org.postgresql.Driver";
        static final String DB_URL = "jdbc:postgresql://localhost:5432/rubis";

        //  Database credentials
        static final String USER = "benchmarksql";
        static final String PASS = "password";

        public static void main(String[] args) {
            System.out.println("Test next power of 2: " + nextPowerOf2(200));

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
                JSONArray json = (new ResultSetData(rs)).getJSON();
                System.out.println("JSON format: " + json);
                System.out.println("MerkleTree: ");

                System.out.println(makeMerkleTree(jsonToLeafs(json)));

                sql = "SELECT * FROM items WHERE id=?";
                stmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                stmt.setInt(1, 2017);
                
                rs = stmt.executeQuery();
                
                json = (new ResultSetData(rs)).getJSON();
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
