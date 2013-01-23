package com.ir.hbase.txn.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import com.ir.constants.TxnConstants;
import com.ir.hbase.txn.TransactionManager;

public class TestConstants extends TxnConstants {

	protected static ZooKeeperWatcher zkw;
	protected static RecoverableZooKeeper rzk;
	protected static HBaseAdmin admin; 
	protected static TransactionManager tm;
	public static Configuration config = HBaseConfiguration.create();

	public static final String testTable1 = "TEST_TABLE1";
	public static final String testTable2 = "TEST_TABLE2";
	public static final String testTable3 = "TEST_TABLE3";
	public static final String testTable4 = "TEST_TABLE4";

	protected static HBaseTestingUtility hbaseTestingUtility = new HBaseTestingUtility(); 

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
	
	public static final String fam1 = "FAMILY1";
	public static final String fam2 = "FAMILY3";
	public static final String fam3 = "FAMILY3";
	
	public static byte[] FAM1 = Bytes.toBytes(fam1);
	public static byte[] FAM2 = Bytes.toBytes(fam2);
	public static byte[] FAM3 = Bytes.toBytes(fam3);

	public static final byte[] VAL1 = Bytes.toBytes("VAL1"); 
	public static final byte[] VAL2 = Bytes.toBytes("VAL2"); 
	public static final byte[] VAL3 = Bytes.toBytes("VAL3"); 
	public static final byte[] VAL4 = Bytes.toBytes("VAL4");
	
	public static final String txnID1 = "/txnIDPath/txnID1";
	
	public static class EmulatedHRegion extends HRegion {
		public String getRegionNameAsString() {
			return "abcdefg";
		}
	}

}
