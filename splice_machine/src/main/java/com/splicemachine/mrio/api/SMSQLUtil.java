package com.splicemachine.mrio.api;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.Txn.IsolationLevel;
import com.splicemachine.si.impl.ReadOnlyTxn;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

public class SMSQLUtil {
    static final Logger LOG = Logger.getLogger(SMSQLUtil.class);
	  private Connection connect = null;
	  private static SMSQLUtil sqlUtil = null;
	  private String connStr = null;
	  public static LazyDataValueFactory factory;
	  
	  static {
		factory = new LazyDataValueFactory();  
	  }
	  
	  private SMSQLUtil(String connStr) throws Exception {  
	      Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
	      this.connStr = connStr;
	      connect = DriverManager.getConnection(connStr);
	  }
	  
	  public static SMSQLUtil getInstance(String connStr){
		  if(sqlUtil == null){
			try {
				sqlUtil = new SMSQLUtil(connStr);		
			} catch (Exception e) {
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
	 * @throws SQLException 
	   * 
	   **/
	  public List<String> getPrimaryKeys(String tableName) throws SQLException {
		  if (LOG.isTraceEnabled())
			  SpliceLogUtils.trace(LOG, "getPrimaryKeys tableName=%s", tableName);
		  	ArrayList<String> names = new ArrayList<String>(1);
			  String[] schemaTable = parseTableName(tableName);
			  ResultSet result = null;
			  try {  
				  DatabaseMetaData databaseMetaData = connect.getMetaData();
			      result = databaseMetaData.getPrimaryKeys(null, schemaTable[0], schemaTable[1]);		     
			      int ordering = -1;
			      while(result.next()){
			    	  names.add(result.getString(4));
			    	  assert ordering<result.getInt(5):"Primary Keys Not Coming Back in Order!";
			    	  ordering = result.getInt(5);
			      }
			  } finally {
				  if (result != null)
				  	result.close();
			  }
			  if (LOG.isTraceEnabled())
				  SpliceLogUtils.trace(LOG, "getPrimaryKeys returns=%s", Arrays.toString(names.toArray()));
			  return names.size()!=0?names:null;
	  }
	  
	  
	  /**
	   * 
	   * Get table structure of 'tableName'
	   * Return Column Name list : Column Type list
	 * @throws SQLException 
	   * 
	   * */
	  public List<NameType> getTableStructure(String tableName) throws SQLException{
		  if (LOG.isTraceEnabled())
			  SpliceLogUtils.trace(LOG, "getTableStructure tableName=%s", tableName);
		  List<NameType> colType = new ArrayList<NameType>();
		  ResultSet result = null;
		  try{
		      String[] schemaTableName = parseTableName(tableName);			  
			  DatabaseMetaData databaseMetaData = connect.getMetaData(); 
		      result = databaseMetaData.getColumns(null, schemaTableName[0],  schemaTableName[1], null);		     
		      while(result.next()){
		    	  colType.add(new NameType(result.getString(4), result.getInt(5)));		          
		      }
		  } finally {
			  if (result != null)
			  	result.close();
		  }
		  if (LOG.isTraceEnabled())
			  SpliceLogUtils.trace(LOG, "getTableStructure returns=%s", Arrays.toString(colType.toArray()));
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
		  String[] schemaTableName = parseTableName(tableName);
          long[] conglomIds = SpliceAdmin.getConglomids(connect, schemaTableName[0], schemaTableName[1]);
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
	  public String getTransactionID() throws SQLException {
		  String trxId = "";
		  ResultSet resultSet = null;
		  try {			
			resultSet = connect.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
			while(resultSet.next()){
				trxId = String.valueOf(resultSet.getLong(1));
			}			
		  } finally {
			  if (resultSet != null)
				  resultSet.close();
		  }
		  return trxId;
	  }
	  
	  public String getTransactionID(Connection conn) throws SQLException{	  
		  String trxId = null;
		  ResultSet resultSet = null;
		  try {
			  resultSet = conn.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
			  while(resultSet.next()){
				long txnID = resultSet.getLong(1);
				trxId = String.valueOf(txnID);
			}
		  } finally {
			  if (resultSet != null)
				  resultSet.close();
		  }
		  return trxId;
	  }
	  
	  public long getChildTransactionID(Connection conn, long parentTxsID, String tableName) throws SQLException {
		  PreparedStatement ps = null;
		  ResultSet rs = null;
		  long childTxsID;
		  try {
			  ps = conn.prepareStatement("call SYSCS_UTIL.SYSCS_START_CHILD_TRANSACTION(?,?)");
			  ps.setLong(1, parentTxsID);
			  ps.setString(2, tableName);
			  ResultSet rs3 = ps.executeQuery();
			  rs3.next();
			  childTxsID = rs3.getLong(1);
		  } finally {
			  if (rs != null)
				  rs.close();
			  if (ps != null)
					  ps.close();
		  }
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
	  
	  public void closeQuietly() {
	    try {
	      if (connect != null) {
	        connect.close();
	      }
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	  }

	  
	  public boolean checkTableExists(String tableName) throws SQLException{
		    ResultSet rs = null;
		    try {
		        DatabaseMetaData meta = connect.getMetaData();
		        String[] schemaTableName = parseTableName(tableName);
		        rs = meta.getTables(null, schemaTableName[0], schemaTableName[1], new String[] { "TABLE" });
		        while (rs.next()) {
		        	return true;
		        }
		        return false;
		    } finally {
		      if (rs != null)
		    	  rs.close();
		    }		
	  }
	  
	  private String[] parseTableName(String str) throws SQLException{
		  str = str.toUpperCase();
		  if(str == null || str.trim().equals(""))
			  return null;
		  else{
			 String[] tmp = str.split("\\.");			
			 if (tmp.length== 2) {
				 return tmp;
			 }
			 if(tmp.length == 1){
				 tmp[1] = tmp[0];
				 tmp[0] = null;
				 return tmp;
			 }
			  throw new SQLException("Splice table not known, "
						+ "please specify Splice tableName. "
						+ "pattern: schemaName.tableName");
		  } 
	  }
	  
	  public int[] getRowDecodingMap(List<NameType> nameTypes, List<String> primaryKeys, List<String> columnNames) {
		  	int []rowDecodingMap = IntArrays.count(nameTypes.size());
			for (int i = 0; i< nameTypes.size(); i++) {
				NameType nameType = nameTypes.get(i);
				if (!primaryKeys.contains(nameType.getName()) && (columnNames.contains(nameType.getName())))
					rowDecodingMap[i] = columnNames.indexOf(nameType.getName());
				else
					rowDecodingMap[i] = -1;
		  	}
			  if (LOG.isTraceEnabled())
				  SpliceLogUtils.trace(LOG, "getRowDecodingMap returns=%s",Arrays.toString(rowDecodingMap));
			return rowDecodingMap;
	  }
	  
	  public int[] getKeyColumnEncodingOrder(List<NameType> nameTypes, List<String> primaryKeys) {  
		    int[] keyColumnEncodingOrder = IntArrays.count(primaryKeys.size());
			for (int i = 0; i< primaryKeys.size(); i++) {
				keyColumnEncodingOrder[i] = locationInNameTypes(nameTypes,primaryKeys.get(i));
			}
			  if (LOG.isTraceEnabled())
				  SpliceLogUtils.trace(LOG, "getKeyColumnEncodingOrder returns=%s",Arrays.toString(keyColumnEncodingOrder));
			return keyColumnEncodingOrder;
	  }
	  
	  public int[] getKeyDecodingMap(int[] keyColumnEncodingOrder, List<String> columnNames, List<NameType> nameTypes) {
			int[] keyDecodingMap = IntArrays.count(keyColumnEncodingOrder.length);
			for (int i = 0; i< keyColumnEncodingOrder.length; i++) {
				keyDecodingMap[i] = columnNames.indexOf(nameTypes.get(keyColumnEncodingOrder[i]).name);
			}
			  if (LOG.isTraceEnabled())
				  SpliceLogUtils.trace(LOG, "getKeyDecodingMap returns=%s",Arrays.toString(keyDecodingMap));
			return keyDecodingMap;		  
	  }
	  
	  public int[] getKeyColumnTypes(int[] keyColumnEncodingOrder, List<NameType> nameTypes) throws StandardException {
			int[] keyColumnTypes = IntArrays.count(keyColumnEncodingOrder.length);
			for (int i = 0; i< keyColumnEncodingOrder.length; i++) {
				keyColumnTypes[i] = nameTypes.get(keyColumnEncodingOrder[i]).getTypeFormatId();
			}
			  if (LOG.isTraceEnabled())
				  SpliceLogUtils.trace(LOG, "getKeyColumnTypes returns=%s",Arrays.toString(keyColumnTypes));
			return keyColumnTypes;
	  }
	  
	  public int[] getExecRowFormatIds(List<String> columnNames, List<NameType> nameTypes) throws StandardException {
			int[] execRowFormatIds = new int[columnNames==null?nameTypes.size():columnNames.size()];
			if (columnNames != null) {
				for (int i = 0; i< columnNames.size(); i++) {
					execRowFormatIds[i] = nameTypes.get(locationInNameTypes(nameTypes,columnNames.get(i))).getTypeFormatId();
				}
			} else {
				for (int i = 0; i< nameTypes.size(); i++) {
					execRowFormatIds[i] = nameTypes.get(i).getTypeFormatId();
				}
			}
			  if (LOG.isTraceEnabled())
				  SpliceLogUtils.trace(LOG, "getExecRowFormatIds returns=%s",execRowFormatIds);
			return execRowFormatIds;
	  }
	  
	  public static ExecRow getExecRow(int[] execRowFormatIds) throws IOException {
		  ExecRow execRow = new ValueRow(execRowFormatIds.length);
		  try {	
		  for (int i = 0; i< execRowFormatIds.length; i++) {
				execRow.setColumn(i+1, factory.getLazyNull(execRowFormatIds[i]));
			}
		  } catch (StandardException se) {
			  throw new IOException(se);
		  }
			  if (LOG.isTraceEnabled())
				  SpliceLogUtils.trace(LOG, "getExecRow returns=%s",execRow);
			return execRow;
	  }
	  
	  
	  public FormatableBitSet getAccessedKeyColumns(int[] keyColumnEncodingOrder,int[] keyDecodingMap) {
		  FormatableBitSet accessedKeyColumns = new FormatableBitSet(keyColumnEncodingOrder.length);
		  for(int i=0;i<keyColumnEncodingOrder.length;i++){
			  int decodingPosition = keyDecodingMap[keyColumnEncodingOrder[i]];
			  if(decodingPosition>=0)
				  accessedKeyColumns.set(i);
		 }
		  if (LOG.isTraceEnabled())
			  SpliceLogUtils.trace(LOG, "getAccessedKeyColumns returns=%s",accessedKeyColumns);
		 return accessedKeyColumns;
	  }
	  private int locationInNameTypes(List<NameType> nameTypes, String name) {
		  for (int i = 0; i< nameTypes.size(); i++) {
			  if (nameTypes.get(i).name.equals(name))
				  return i;
		  }
		  throw new RuntimeException("misssing element");
	  }
	  
	  
	  public TableScannerBuilder getTableScannerBuilder(String tableName, List<String> columnNames) throws SQLException {
		List<String> primaryKeys = getPrimaryKeys(tableName); 
		List<NameType> nameTypes = getTableStructure(tableName);
		if (columnNames ==null) {
			columnNames = new ArrayList<String>(nameTypes.size());
			for (int i = 0; i< nameTypes.size(); i++) {
				columnNames.add(nameTypes.get(i).name);
			}
		}
		int[] rowDecodingMap = getRowDecodingMap(nameTypes,primaryKeys,columnNames);
		int[] keyColumnEncodingOrder = getKeyColumnEncodingOrder(nameTypes,primaryKeys);
		boolean[] keyColumnOrdering = new boolean[keyColumnEncodingOrder.length];
		for (int i = 0; i< keyColumnEncodingOrder.length; i++) {
			keyColumnOrdering[i] = true;
		}
		int[] keyDecodingMap = getKeyDecodingMap(keyColumnEncodingOrder,columnNames,nameTypes);		
		int[] keyColumnTypes;
		int[] execRowFormatIds;
		try {
			execRowFormatIds = getExecRowFormatIds(columnNames, nameTypes);
			keyColumnTypes = getKeyColumnTypes(keyColumnEncodingOrder,nameTypes);
		} catch (StandardException e) {
			throw new SQLException(e);
		}
		FormatableBitSet accessedKeyColumns = getAccessedKeyColumns(keyColumnEncodingOrder,keyDecodingMap);			
			return new TableScannerBuilder()
        .transaction(ReadOnlyTxn.create(Long.parseLong(getTransactionID()),IsolationLevel.SNAPSHOT_ISOLATION, null))
							.metricFactory(Metrics.basicMetricFactory())
							.scan(createNewScan())
							.execRowTypeFormatIds(execRowFormatIds)
							.tableVersion("2.0")
							.indexName(null)
							.keyColumnEncodingOrder(keyColumnEncodingOrder)
							.keyColumnSortOrder(null)
							.keyColumnTypes(keyColumnTypes)
							.accessedKeyColumns(accessedKeyColumns)
							.keyDecodingMap(keyDecodingMap)
							.keyColumnSortOrder(keyColumnOrdering)
							.rowDecodingMap(rowDecodingMap);		
	  }
	
	  public void close() throws SQLException {
		  if (connect!=null)
			  connect.close();
		  
	  }
	  
	  public static Scan createNewScan() {
	    	Scan scan = new Scan();
	    	scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);
	    	scan.setMaxVersions();
	    	scan.setCaching(SIConstants.DEFAULT_CACHE_SIZE);
	    	return scan;
	  }
	  
}
