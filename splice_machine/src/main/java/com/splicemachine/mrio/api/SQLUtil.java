/**
 * SQLUtil which is a wrapper of Splice(Derby layer)
 * @author Yanan Jian
 * Created on: 08/14/14
 */
package com.splicemachine.mrio.api;

import com.splicemachine.derby.utils.SpliceAdmin;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
	  private static SQLUtil sqlUtil = null;
	  private String connStr = null;
	  
	  private SQLUtil(String connStr) throws Exception {  
	      Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
	      this.connStr = connStr;
	      connect = DriverManager.getConnection(connStr);
	  }
	  
	  public static SQLUtil getInstance(String connStr){
		  if(sqlUtil == null){
			try {
				sqlUtil = new SQLUtil(connStr);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				throw new RuntimeException(e);
			}
		  }
		  
			return sqlUtil;
	  }
	  
	  
	  public Connection createConn() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		  Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
		  
		  Connection conn = DriverManager.getConnection(connStr);
		  return conn;
	  }
	  
	  public void disableAutoCommit(Connection conn) throws SQLException{
		  conn.setAutoCommit(false);
	  }
	  
	  public void commit(Connection conn) throws SQLException{
		  conn.commit();
	  }
	  
	  public void rollback(Connection conn) throws SQLException{
		  conn.rollback();
	  }
	  
	  /**
	   * Get primary key from 'tableName'
	   * Return Column Name list : Column Seq list
	   * Column Id here refers to Column Seq, where Id=1 means the first column in primary keys.
	   * 
	   **/
	  public HashMap<List, List> getPrimaryKey(String tableName){
		  //System.out.println("getting pk");
		  HashMap<List, List> pks = new HashMap<List, List>();
		  ArrayList<String> names = new ArrayList<String>();
		  ArrayList<Integer> types = new ArrayList<Integer>();
		  try{
			  
			  String   catalog           = null;
		      String   schemaPattern     = null;
		      String   tableNamePattern  = null;
		      
			  HashMap<String, String> schema_tblName = parseTableName(tableName);
			  
			  if (schema_tblName != null)
			  {
				  Map.Entry pairs = (Map.Entry)schema_tblName.entrySet().iterator().next();
				  schemaPattern = (String) pairs.getKey();
				  tableNamePattern = (String) pairs.getValue();
			  }
			  else
				  throw new SQLException("Splice table not known, "
				  							+ "please specify Splice tableName. "
				  							+ "pattern: schemaName.tableName");
		      
			  DatabaseMetaData databaseMetaData = connect.getMetaData();

		      ResultSet result = databaseMetaData.getPrimaryKeys(catalog, schemaPattern, tableNamePattern);
		      
		      while(result.next()){
		    	  
		          String columnName = result.getString(4);
		          int    columnId = result.getInt(5);
		          
		          names.add(columnName);
		          types.add(columnId);
		          
		      }
		     
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
	  public HashMap<List, List> getTableStructure(String tableName){
		  //System.out.println("getting table structure");
		  HashMap<List, List> colType = new HashMap<List, List>();
		  ArrayList<String> names = new ArrayList<String>();
		  ArrayList<Integer> types = new ArrayList<Integer>();
		  try{
			 
			  String   catalog           = null;
		      String   schemaPattern     = null;
		      String   tableNamePattern  = tableName;
		      String   columnNamePattern = null;
		      HashMap<String, String> schema_tblName = parseTableName(tableName);
			  
			  if (schema_tblName != null){
				  Map.Entry pairs = (Map.Entry)schema_tblName.entrySet().iterator().next();
				  schemaPattern = (String) pairs.getKey();
				  tableNamePattern = (String) pairs.getValue();
			  }
			  else
				  throw new SQLException("Splice table not known, please specify Splice tableName. "
				  							+ "pattern: schemaName.tableName");
			  DatabaseMetaData databaseMetaData = connect.getMetaData(); 

		      ResultSet result = databaseMetaData.getColumns(
		          catalog, schemaPattern,  tableNamePattern, columnNamePattern);
		     
		      while(result.next()){
		          String columnName = result.getString(4);
		          int    columnType = result.getInt(5);
		          names.add(columnName); 
		          types.add(columnType);
		          
		      }
		      
		      colType.put(names, types);
		    } catch (Exception e) {
		      System.out.println(e);
		    } 
		  return colType;
	  }
	  
	  public Connection getStaticConnection(){
		  return this.connect;
	  }
	  
	  /**
	   * 
	   * Get ConglomID from 'tableName'
	   * Param is Splice tableName with schema, pattern: schema.tableName
	   * Return ConglomID
	   * ConglomID means HBase table Name which maps to the Splice table Name
	 * @throws SQLException 
	   * 
	   * */
	  public String getConglomID(String tableName) throws SQLException{
		  String schema = null;
		  String tblName = null;
		  HashMap<String, String> schema_tblName = parseTableName(tableName);
		  
		  if (schema_tblName != null){
			  Map.Entry pairs = (Map.Entry)schema_tblName.entrySet().iterator().next();
			  schema = (String) pairs.getKey();
			  tblName = (String) pairs.getValue();
		  }
		  else
			  throw new SQLException("Splice table not known, please specify Splice tableName. "
			  							+ "pattern: schemaName.tableName");
          long[] conglomIds = SpliceAdmin.getConglomids(connect, schema, tblName);
          StringBuffer str = new StringBuffer();
          str.append(conglomIds[0]);

          return str.toString();
	  }
	  
	  /**
	   * 
	   * Get TransactionID
	   * Each Transaction has a uniq ID
	   * For every map job, there should be a different transactionID.
	   * 
	   * */
	  public String getTransactionID(){
		  String trxId = ""; 
		  try {
			
			resultSet = connect.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
			while(resultSet.next()){
				trxId = String.valueOf(resultSet.getLong(1));
			}
			
		  } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  return trxId;
	  }
	  
	  public String getTransactionID(Connection conn) throws SQLException{	  
		  String trxId = null;
		  resultSet = conn.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
			while(resultSet.next()){
				long txnID = resultSet.getLong(1);
				trxId = String.valueOf(txnID);
			}
		  return trxId;
	  }
	  
	  public long getChildTransactionID(Connection conn, long parentTxsID, String tableName) throws SQLException {
		  PreparedStatement ps = conn.prepareStatement("call SYSCS_UTIL.SYSCS_START_CHILD_TRANSACTION(?,?)");
		  ps.setLong(1, parentTxsID);
		  //ps.setLong(2, conglomId);
		  ps.setString(2, tableName);
		  ResultSet rs3 = ps.executeQuery();
		  rs3.next();
		  long childTxsID = rs3.getLong(1);
		  System.out.println("childTxsID:"+childTxsID);
		  return childTxsID;
	  }

    public void commitChildTransaction(Connection conn, long childTxnID) throws SQLException{
        PreparedStatement ps = conn.prepareStatement("call SYSCS_UTIL.SYSCS_COMMIT_CHILD_TRANSACTION(?)");
        ps.setLong(1, childTxnID);
        ps.execute();
    }


    public void closeConn(Connection conn) throws SQLException{
		  conn.close();
	  }
	  
	  public void close() {
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
	    	e.printStackTrace();
	    }
	  }

	  
	  public boolean checkTableExists(String tableName) throws SQLException{
		  boolean tableExists = false;
		  
		    ResultSet rs = null;
		    try {
		        DatabaseMetaData meta = connect.getMetaData();
		        HashMap<String, String> schema_tblName = parseTableName(tableName);
				String schema = null;
				String tblName = null;
				if (schema_tblName != null){
					Map.Entry pairs = (Map.Entry)schema_tblName.entrySet().iterator().next();
					schema = (String) pairs.getKey();
					tblName = (String) pairs.getValue();
				 }
				 else
					throw new SQLException("Splice table not known, "
											+ "please specify Splice tableName. "
					  						+ "pattern: schemaName.tableName");
		        rs = meta.getTables(null, schema, tblName, new String[] { "TABLE" });
		        while (rs.next()) {
		            String currentTableName = rs.getString("TABLE_NAME");
		            if (currentTableName.equalsIgnoreCase(tblName)) {
		                tableExists = true;
		            }
		        }
		    } catch (SQLException e) {
		     
		    } finally {
		        
		    }
		 
		    return tableExists;
		
	  }
	  
	  private HashMap<String, String> parseTableName(String str){
		  
		  if(str == null || str.trim().equals(""))
			  return null;
		  else{
			 HashMap<String, String> res = new HashMap<String, String>();
			 
			 String[]tmp = str.split("\\.");
			
			 String schema = "SPLICE";
			 String tableName = str;
			 if(tmp.length >= 2){
				 schema = tmp[0];
				 tableName = tmp[1];
			 }
			  res.put(schema, tableName);
			  return res;
		  } 
	  }
	  
	
}
