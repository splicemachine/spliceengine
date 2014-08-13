package com.splicemachine.mrio.api;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class SQLUtil {
	private Connection connect = null;
	  private Statement statement = null;
	  private ResultSet resultSet = null;
	  private static SQLUtil derbyTest = null;
	  
	  private SQLUtil() throws Exception {  
	      Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
	      connect = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb");
	      
	  }
	  
	  public static SQLUtil getInstance()
	  {
		  if(derbyTest == null)
			try {
				derbyTest = new SQLUtil();
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  finally
		  {
			  return derbyTest;
		  }
		  else
			  return derbyTest;
	  }
	  
	  
	  public Connection createConn() throws SQLException
	  {
		  Connection conn = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb");
		  return conn;
	  }
	  
	  public void disableAutoCommit(Connection conn) throws SQLException
	  {
		  conn.setAutoCommit(false);
	  }
	  
	  public void commit(Connection conn) throws SQLException
	  {
		  conn.commit();
	  }
	  
	  public void rollback(Connection conn) throws SQLException
	  {
		  conn.rollback();
	  }
	  
	  /**
	   * Get primary key from 'tableName'
	   * Return Column Name list : Column Seq list
	   * Column Id here refers to Column Seq, where Id=1 means the first column in primary keys.
	   * 
	   **/
	  public HashMap<List, List> getPrimaryKey(String tableName)
	  {
		  HashMap<List, List> pks = new HashMap<List, List>();
		  ArrayList names = new ArrayList();
		  ArrayList types = new ArrayList();
		  try{
			  long start = System.currentTimeMillis();
			  String   catalog           = null;
		      String   schemaPattern     = null;
		      String   tableNamePattern  = tableName;
		      
			  DatabaseMetaData databaseMetaData = connect.getMetaData();

		      ResultSet result = databaseMetaData.getPrimaryKeys(catalog, schemaPattern, tableNamePattern);
		      
		      while(result.next()){
		    	  
		          String columnName = result.getString(4);
		          int    columnId = result.getInt(5);
		          System.out.println("ColumnName:"+columnName+" Id:"+String.valueOf(columnId));
		          names.add(columnName);
		          types.add(columnId);
		          
		      }
		      long elapsedTimeMillis = System.currentTimeMillis()-start;
		      System.out.println("Elapsed time:"+String.valueOf(elapsedTimeMillis));
		      pks.put(names, types);
		    } catch (Exception e) {
		      System.out.println(e);
		    } 
		  return pks;
	  }
	  
	  
	  /**
	   * 
	   * Get table structure of 'tableName'
	   * Return Column Name list : Column Type list
	   * 
	   * */
	  public HashMap<List, List> getTableStructure(String tableName)
	  {
		  HashMap<List, List> colType = new HashMap<List, List>();
		  ArrayList names = new ArrayList();
		  ArrayList types = new ArrayList();
		  try{
			  long start = System.currentTimeMillis();
			  String   catalog           = null;
		      String   schemaPattern     = null;
		      String   tableNamePattern  = tableName;
		      String   columnNamePattern = null;
		      
			  DatabaseMetaData databaseMetaData = connect.getMetaData();

		      ResultSet result = databaseMetaData.getColumns(
		          catalog, schemaPattern,  tableNamePattern, columnNamePattern);
		      
		      while(result.next()){
		          String columnName = result.getString(4);
		          int    columnType = result.getInt(5);
		          
		          System.out.println("ColumnName:"+columnName+" Type:"+String.valueOf(columnType));
		          names.add(columnName);
		          types.add(columnType);
		          
		      }
		      long elapsedTimeMillis = System.currentTimeMillis()-start;
		      System.out.println("Elapsed time:"+String.valueOf(elapsedTimeMillis));
		      colType.put(names, types);
		    } catch (Exception e) {
		      System.out.println(e);
		    } 
		  return colType;
	  }
	  
	  public Connection getStaticConnection()
	  {
		  return this.connect;
	  }
	  
	  /**
	   * 
	   * Get ConglomID from 'tableName'
	   * Param is Splice tableName
	   * Return ConglomID
	   * ConglomID means HBase table Name which maps to the Splice table Name
	   * 
	   * */
	  public String getConglomID(String tableName)
	  {
		  String conglom_id = null;
		  String query = "select s.schemaname,t.tablename,c.conglomeratenumber "+
		                 "from sys.sysschemas s, sys.systables t, sys.sysconglomerates c "+
				         "where s.schemaid = t.schemaid and "+
		                 "t.tableid = c.tableid and "+
				         "s.schemaname = 'SPLICE' and "+
		                 "t.tablename = '"+tableName+"'";
		  PreparedStatement statement;
		try {
			statement = connect.prepareStatement(query);
			resultSet = statement.executeQuery();
		    while (resultSet.next()) {
		        conglom_id = resultSet.getString("CONGLOMERATENUMBER");   
		        break;
		      }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	  
	      return conglom_id;
	  }
	  
	  /**
	   * 
	   * Get TransactionID
	   * Each Transaction has a uniq ID
	   * For every map job, there should be a different transactionID.
	   * 
	   * */
	  public String getTransactionID()
	  {
		  String trxId = ""; 
		  try {
			
			resultSet = connect.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
			while(resultSet.next())
			{
				trxId = String.valueOf(resultSet.getInt(1));
			}
			
		  } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  return trxId;
	  }
	  
	  public String getTransactionID(Connection conn) throws SQLException
	  {	  
		  String trxId = null;
		  resultSet = conn.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
			while(resultSet.next())
			{
				trxId = String.valueOf(resultSet.getInt(1));
			}
			
		  return trxId;
	  }
	  
	  public String getChildTransactionID(Connection conn, String parentTxsID, long conglomId) throws SQLException
	  {
		  PreparedStatement ps = conn.prepareStatement("call SYSCS_UTIL.SYSCS_START_CHILD_TRANSACTION(?, ?)");
		  long ptxsID = Long.parseLong(parentTxsID);
		  ps.setLong(1, ptxsID);
		  ps.setLong(2, conglomId);
		  ResultSet rs3 = ps.executeQuery();
		  rs3.next();
		  long childTxsID = rs3.getLong(1);
		  return new Long(childTxsID).toString();
	  }
	  
	  private void close() {
	    try {
	      if (resultSet != null) {
	        resultSet.close();
	      }
	      if (statement != null) {
	        statement.close();
	      }
	      if (connect != null) {
	        connect.close();
	      }
	    } catch (Exception e) {

	    }
	  }

	  
	  public boolean checkTableExists(String tableName)
	  {
		  boolean tableExists = false;
		  
		    ResultSet rs = null;
		    try {
		        DatabaseMetaData meta = connect.getMetaData();
		        rs = meta.getTables(null, null, null, new String[] { "TABLE" });
		        while (rs.next()) {
		            String currentTableName = rs.getString("TABLE_NAME");
		            if (currentTableName.equalsIgnoreCase(tableName)) {
		                tableExists = true;
		            }
		        }
		    } catch (SQLException e) {
		     
		    } finally {
		        
		    }
		 
		    return tableExists;
		
	  }
	  
	  public static void main(String[] args) throws Exception {
	    SQLUtil dao = SQLUtil.getInstance();
	    HashMap<List, List> structure = dao.getPrimaryKey("USERTEST8");
	    Iterator iter = structure.entrySet().iterator();
	    while(iter.hasNext())
	    {
	    	Map.Entry kv = (Map.Entry)iter.next();
	    	ArrayList<String> names = (ArrayList<String>)kv.getKey();
	    	ArrayList<Integer> types = (ArrayList<Integer>)kv.getValue();
	    	
	    	System.out.println("names: "+names+","+" types: "+types);
	    }
	    dao.getTransactionID();
	  }
}
