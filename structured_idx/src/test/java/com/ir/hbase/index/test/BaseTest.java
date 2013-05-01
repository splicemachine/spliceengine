package com.ir.hbase.index.test;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;

import com.ir.constants.SpliceConstants;
import com.ir.hbase.client.index.IndexColumn;
import com.ir.hbase.client.structured.Column;
@Ignore
public class BaseTest extends SpliceConstants {

	protected static HTable htable;
	protected static HTable indexTable;
	protected static HBaseTestingUtility hbaseTestingUtility = new HBaseTestingUtility(); 
	protected static HBaseAdmin admin;

	public static final String regTable1 = "REGULAR_TABLE1";
	public static final byte[] REGULAR_TABLE1 = Bytes.toBytes(regTable1);
	public static final String SINGLE_INTEGER_INDEX = "Integer_Ascending";
	public static final String INDEX1 = "Letter:Descending_Integer:Ascending_Index";
	public static final String INDEX2 = "Bool:Ascending_Long:Descending_Double:Ascending_Index";

	public static final String row2 = "ROW2";
	public static final String row3 = "ROW3";
	public static final byte[] ROW2 = Bytes.toBytes(row2);
	public static final byte[] ROW3 = Bytes.toBytes(row3);

	public static final String colfam1 = "FAMILY1";
	public static final String colfam2 = "FAMILY2";
	public static final String colfam3 = "FAMILY3";
	public static final String colfam4 = "FAMILY4";
	public static final String colfam5 = "FAMILY5";
	public static final String colfam6 = "FAMILY6";
	public static final String colfam7 = "FAMILY7";
	public static final String colfam8 = "FAMILY8";
	public static final String colfam9 = "FAMILY9";

	public static final byte[] FAMILY1 = Bytes.toBytes(colfam1); 
	public static final byte[] FAMILY2 = Bytes.toBytes(colfam2); 
	public static final byte[] FAMILY3 = Bytes.toBytes(colfam3);
	public static final byte[] FAMILY4 = Bytes.toBytes(colfam4);
	public static final byte[] FAMILY5 = Bytes.toBytes(colfam5);
	public static final byte[] FAMILY6 = Bytes.toBytes(colfam6);
	public static final byte[] FAMILY7 = Bytes.toBytes(colfam7);
	public static final byte[] FAMILY8 = Bytes.toBytes(colfam8);
	public static final byte[] FAMILY9 = Bytes.toBytes(colfam9);

	public static final String col1 = "COL1";
	public static final String col2 = "COL2";
	public static final String col3 = "COL3";
	public static final String col4 = "COL4";
	public static final String col5 = "COL5";
	public static final String col6 = "COL6";
	public static final String col7 = "COL7";
	public static final String col8 = "COL8";
	public static final String col9 = "COL9";

	public static final byte[] COL1 = Bytes.toBytes(col1); 
	public static final byte[] COL2 = Bytes.toBytes(col2); 
	public static final byte[] COL3 = Bytes.toBytes(col3); 
	public static final byte[] COL4 = Bytes.toBytes(col4); 
	public static final byte[] COL5 = Bytes.toBytes(col5); 
	public static final byte[] COL6 = Bytes.toBytes(col6);
	public static final byte[] COL7 = Bytes.toBytes(col7);
	public static final byte[] COL8 = Bytes.toBytes(col8);
	public static final byte[] COL9 = Bytes.toBytes(col9);

	public static final byte[] VAL1 = Bytes.toBytes("VAL1"); 
	public static final byte[] VAL2 = Bytes.toBytes("VAL2"); 
	public static final byte[] VAL3 = Bytes.toBytes("VAL3"); 
	public static final byte[] VAL4 = Bytes.toBytes("VAL4");
	
	public static final Column column1 = new Column(colfam2, col3);
	public static final Column column2 = new Column(colfam2, col4);
	public static final Column column3 = new Column(colfam3, col5);
	public static final Column column4 = new Column(colfam3, col6);
	public static final IndexColumn icolumn1 = new IndexColumn(colfam1, col1, IndexColumn.Order.DESCENDING, Column.Type.STRING);
	public static final IndexColumn icolumn2 = new IndexColumn(colfam1, col2, IndexColumn.Order.ASCENDING, Column.Type.INTEGER);
	public static final IndexColumn icolumn3 = new IndexColumn(colfam4, col7, IndexColumn.Order.ASCENDING, Column.Type.BOOLEAN);
	public static final IndexColumn icolumn4 = new IndexColumn(colfam4, col8, IndexColumn.Order.DESCENDING, Column.Type.LONG);
	public static final IndexColumn icolumn5 = new IndexColumn(colfam4, col9, IndexColumn.Order.ASCENDING, Column.Type.DOUBLE);

	public static void basicConf() throws Exception {
		hbaseTestingUtility.getConfiguration().set("hbase.zookeeper.property.clientPort", "21818");
		hbaseTestingUtility.getConfiguration().reloadConfiguration();
	}

	public static void loadIdxCoprocessor() {
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.master.classes", "com.ir.hbase.coprocessor.index.IndexMasterObserver");
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.region.classes", "com.ir.hbase.coprocessor.index.IndexRegionObserver");
		hbaseTestingUtility.getConfiguration().reloadConfiguration();
	}

	public static void loadStructureCoprocessor() {
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.master.classes", "com.ir.hbase.coprocessor.structured.StructuredMasterObserver");
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.region.classes", "com.ir.hbase.coprocessor.structured.StructuredRegionObserver");
		hbaseTestingUtility.getConfiguration().reloadConfiguration();
	}

	public static void loadTxnCoprocessor() {
		hbaseTestingUtility.getConfiguration().set("hbase.zookeeper.property.clientPort", "21818");
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.region.classes", "com.ir.hbase.txn.coprocessor.region.TransactionalManagerRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalRegionObserver");
		hbaseTestingUtility.getConfiguration().reloadConfiguration();
	}

	public static void loadIdxTxnCoprocessor() {
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.master.classes", "com.ir.hbase.coprocessor.index.IndexMasterObserver");
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.region.classes", "com.ir.hbase.coprocessor.index.IndexRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalManagerRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalRegionObserver");
		hbaseTestingUtility.getConfiguration().reloadConfiguration();
	}
	
	public static void loadIdxStructuredCoprocessor() {
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.master.classes", "com.ir.hbase.coprocessor.index.IndexMasterObserver,com.ir.hbase.coprocessor.structured.StructuredMasterObserver");
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.region.classes", "com.ir.hbase.coprocessor.index.IndexRegionObserver,com.ir.hbase.coprocessor.structured.StructuredRegionObserver");
		hbaseTestingUtility.getConfiguration().reloadConfiguration();
	}
	
	public static void loadIdxStructuredTxnCoprocessor() {
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.master.classes", "com.ir.hbase.coprocessor.index.IndexMasterObserver,com.ir.hbase.coprocessor.structured.StructuredMasterObserver");
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.region.classes", "com.ir.hbase.coprocessor.index.IndexRegionObserver,com.ir.hbase.coprocessor.structured.StructuredRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalManagerRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalRegionObserver");
		hbaseTestingUtility.getConfiguration().reloadConfiguration();
	}

	public static void start() throws Exception {
		hbaseTestingUtility.startMiniCluster(1);
	}
	public static void end() throws Exception {
		hbaseTestingUtility.shutdownMiniCluster();		
	}


}
