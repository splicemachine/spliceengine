package com.splicemachine.mrio.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBlob;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLReal;
import org.apache.derby.iapi.types.SQLSmallint;
import org.apache.derby.iapi.types.SQLTime;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.mrio.api.SQLUtil;
import com.splicemachine.mrio.api.SpliceTableScanner;
import com.splicemachine.mrio.api.SpliceTableScannerBuilder;
import com.splicemachine.stats.Metrics;
import com.splicemachine.utils.IntArrays;

public class SpliceRecordReaderTest {

	public void createTable(String tableName, String sqlStat)
	{
		try {
			Connection conn = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb");
			Statement stmt = conn.createStatement();
			stmt.execute(sqlStat);
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean ifTableExists(String tableName)
	{
		String sqlStat = "";
		Connection conn;
		boolean exists = false;
		try {
			conn = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb");
			DatabaseMetaData dbmd = conn.getMetaData();
	        ResultSet rs = dbmd.getTables(null, null, tableName.toUpperCase(),null);
	        if(rs.next())
	        {
	            System.out.println("Table "+rs.getString("TABLE_NAME")+"already exists !!");
	            exists = true;
	        }
	       
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return exists;
	}
	public void insertData(String tableName)
	{
		try {
			Connection conn = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb");
			Statement stmt = conn.createStatement();
			String val1 = "word";
			String val2 = "randc";
			String val3 = "100";
			
			String query = "insert into "+  
					tableName +" values('"+val1 + "','"+val2 + "',"+val3+")";
        	System.out.println(query);
        	stmt.executeUpdate(query);
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		
	}
	public void testRead()
	{
		String tableName = "TESTREAD";
		ArrayList<String> pkColNames;
		ArrayList<Integer> pkColIds;
		
		String sqlStat = "create table "+tableName
										+" (WORD varchar(100) not null,"
										+ "RANDC varchar(100) not null, "
										+ "RANDI integer not null, "
										+ "primary key(RANDC, RANDI))";
		
		if(!ifTableExists(tableName))
			createTable(tableName, sqlStat);
		
		SQLUtil sqlUtil = SQLUtil.getInstance();
		HashMap<List, List> pks = sqlUtil.getPrimaryKey(tableName);
		Iterator iterpk = pks.entrySet().iterator();
		//iterpk.next();
		   
	    Map.Entry kv2 = (Map.Entry)iterpk.next();
	    pkColNames = (ArrayList<String>)kv2.getKey();
	    pkColIds = (ArrayList<Integer>)kv2.getValue();
	    
	    System.out.println(pkColNames);
	    System.out.println(pkColIds);
	    insertData(tableName);
	}

	public void testReadOutofOrderPK() throws IOException, StandardException
	{
		String tableName = "TESTREAD";
		ArrayList<String> pkColNames = null;
		ArrayList<Integer> pkColIds = null;
		ArrayList<String> colNames = null;
		ArrayList<Integer> colTypes = null;
		
		String sqlStat = "create table "+tableName
										+" (WORD varchar(100) not null,"
										+ "RANDC varchar(100) not null, "
										+ "RANDI integer not null, "
										+ "primary key(RANDI, RANDC))";
		
		if(!ifTableExists(tableName))
			createTable(tableName, sqlStat);
		
		SQLUtil sqlUtil = SQLUtil.getInstance();
		HashMap<List, List> pks = sqlUtil.getPrimaryKey(tableName);
		Iterator iterpk = pks.entrySet().iterator();
		
		if(iterpk.hasNext()) 
	    {
			Map.Entry kv2 = (Map.Entry)iterpk.next();
			pkColNames = (ArrayList<String>)kv2.getKey();
			pkColIds = (ArrayList<Integer>)kv2.getValue();
	    }
	    
	    HashMap<List, List> tableStructure = sqlUtil.getTableStructure(tableName);
	    Iterator iterst = tableStructure.entrySet().iterator();
	    if(iterst.hasNext())
	    {
	    	Map.Entry kv = (Map.Entry)iterst.next();
			colNames = (ArrayList<String>)kv.getKey();
			colTypes = (ArrayList<Integer>)kv.getValue();
	    }
	    
	    System.out.println(pkColNames);
	    System.out.println(pkColIds);
	    System.out.println(colNames);
	    System.out.println(colTypes);
	    insertData(tableName);
	    String htableName = sqlUtil.getConglomID(tableName);
	    testReadingData(colNames, colTypes, pkColNames, pkColIds, htableName);
	}

	private void printIntList(int []intlist, String name)
	{
		for(int i = 0; i < intlist.length; i++)
		{
			System.out.println(name + " "+intlist[i]);
		}
	}
	
	public void testReadingData(ArrayList<String> colNames, 
								ArrayList<Integer> colTypes, 
								ArrayList<String> pkColNames,
								ArrayList<Integer> pkColIds, 
								String hbaseTableName) throws IOException, StandardException
	{
		int[] rowEncodingMap;
		int[] rowDecodingMap;
		int []keyColumnOrder = new int[pkColIds.size()];
		int []keyDecodingMap = new int[pkColIds.size()];
		String txsId = SQLUtil.getInstance().getTransactionID();
		Scan scan = new Scan();
		scan.setMaxVersions();
		scan.setAttribute(SIConstants.SI_EXEMPT, Bytes.toBytes(true));
		ExecRow row = new ValueRow(colTypes.size());
		rowEncodingMap = IntArrays.count(colTypes.size());
		rowDecodingMap = rowEncodingMap;
		HTable htable = new HTable(Bytes.toBytes(hbaseTableName));
		ResultScanner scanner = htable.getScanner(scan);
		for (int i = 0 ; i < pkColIds.size(); i++)
		{
			keyColumnOrder[i] = pkColIds.get(i)-1;
			keyDecodingMap[i] = pkColIds.get(i);
		}
		System.out.println("keyColumnOrder: ");
		printIntList(keyColumnOrder, "keyColumnOrder");
		printIntList(keyDecodingMap, "keyDecodingMap");
		//keyDecodingMap[0] = 2;
		//keyDecodingMap[1] = 1;
		FormatableBitSet accessedKeyCols = new FormatableBitSet(colTypes.size());
		for(int i = 0; i< keyColumnOrder.length; i++)
		{
			if(keyDecodingMap[i] >= 0)
				accessedKeyCols.set(i);
		}
		int[] keyColumnTypes = null;
		int[] keyEncodingMap = null;
		if(keyColumnOrder!=null){
			
			if(keyEncodingMap==null){
				keyEncodingMap = new int[keyColumnOrder.length];
				for(int i=0;i<keyColumnOrder.length;i++){
					keyEncodingMap[i] = keyDecodingMap[keyColumnOrder[i]];
				}
				
			}
			keyColumnTypes = new int[keyColumnOrder.length];
			for(int i=0;i<keyEncodingMap.length;i++){
				if(keyEncodingMap[i] < 0)
					continue;
				keyColumnTypes[i] = getTypeFormatId(colTypes.get(keyEncodingMap[i]));	
			}
			rowEncodingMap = IntArrays.count(colTypes.size());
			for(int pkCol:keyEncodingMap){
				
				rowEncodingMap[pkCol] = -1;
			}
			
			
				
		}

		SpliceTableScannerBuilder builder = new SpliceTableScannerBuilder()
		.scan(scan)
		.scanner(scanner)	
		.metricFactory(Metrics.basicMetricFactory())
		.transactionID(txsId)
		.tableVersion("2.0") // should read table version from derby metadata table
		.rowDecodingMap(rowDecodingMap)
		.template(row.getNewNullRow())
		.indexName(null)
		.setHtable(htable)
		.setColumnTypes(colTypes)
    	
    	.keyColumnEncodingOrder(keyColumnOrder)
    	.keyDecodingMap(keyDecodingMap)
    	.keyColumnTypes(keyColumnTypes)	
    	.accessedKeyColumns(accessedKeyCols);
		
		SpliceTableScanner tableScanner = builder.build();
		ExecRow rowdata = tableScanner.next(null);
		
		decode(rowdata);

	}
	
	public void decode(ExecRow row) throws StandardException
    {
    	DataValueDescriptor dvd[]  = row.getRowArray();
    	for(DataValueDescriptor data : dvd)
    	{
    	
    		String tp = data.getTypeName();
    		if (data.isNull())
    		{
    			System.out.println("Column Value NULL");
    			continue;
    		}
    		
    		if(tp.equals("INTEGER"))
    			System.out.println(data.getInt());
    		else if(tp.equals("VARCHAR"))
    			System.out.println(data.getString());
    		else if(tp.equals("FLOAT"))
    			System.out.println(data.getFloat());
    		else if(tp.equals("DOUBLE"))
    			System.out.println(data.getDouble());
    		else
    			System.out.println(tp);
    	}
    }
	
	private int getTypeFormatId(int columnType)
    {
    	switch(columnType)
		{
				case java.sql.Types.INTEGER:
					return new SQLInteger(1).getTypeFormatId();
				case java.sql.Types.BIGINT:
					return new SQLLongint(1).getTypeFormatId();				
				case java.sql.Types.TIMESTAMP:
					return new SQLTimestamp().getTypeFormatId();
				case java.sql.Types.TIME:
					return new SQLTime().getTypeFormatId();					
				case java.sql.Types.SMALLINT:
					return new SQLSmallint().getTypeFormatId();
				case java.sql.Types.BOOLEAN:
					return new SQLBoolean().getTypeFormatId();
				case java.sql.Types.DOUBLE:
					return new SQLDouble().getTypeFormatId();
				case java.sql.Types.FLOAT:
					return new SQLReal().getTypeFormatId();
				case java.sql.Types.CHAR:
					return new SQLChar().getTypeFormatId();
				case java.sql.Types.VARCHAR:
					return new SQLVarchar().getTypeFormatId();
				case java.sql.Types.BINARY:
					return new SQLBlob().getTypeFormatId();
				default:
					return new org.apache.derby.iapi.types.SQLClob().getTypeFormatId();
		}
    }
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SpliceRecordReaderTest rrt = new SpliceRecordReaderTest();
		rrt.testRead();
		try {
			rrt.testReadOutofOrderPK();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StandardException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
