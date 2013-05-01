package com.splicemachine.hbase.server.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.BeforeClass;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.txn.TxnConstants;
import com.splicemachine.hbase.txn.ZkTransactionManager;

public class BaseTestOnServer extends TxnConstants {
    
	protected static ZooKeeperWatcher zkw;
	protected static RecoverableZooKeeper rzk;
	protected static HBaseAdmin admin; 
	protected static ZkTransactionManager tm;
    public static Configuration config = HBaseConfiguration.create();

	public static final String testTable1 = "TEST_TABLE1";
	public static final String testTable2 = "TEST_TABLE2";
	public static final String testTable3 = "TEST_TABLE3";
	public static final String testTable4 = "TEST_TABLE4";
	
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
	
	@BeforeClass
	@SuppressWarnings(value = "deprecation")
	public static void setUp() throws Exception {
		admin = new HBaseAdmin(config);		
		zkw = admin.getConnection().getZooKeeperWatcher();
		rzk = zkw.getRecoverableZooKeeper();
		tm = new ZkTransactionManager(config, zkw, rzk);
	}
	
	public static HTable getTestTable(HBaseAdmin admin, String tableName) throws Exception {
		if (!admin.tableExists(tableName)) {
			return createTable(admin, tableName); 
		} else {
			return new HTable(config, tableName);
		}
	}
	
	public static HTable createTable(HBaseAdmin admin, String tableName) throws Exception {
		HTableDescriptor desc = new HTableDescriptor(tableName);
//		String compression = admin.getConfiguration().get(SpliceConstants.TABLE_COMPRESSION,SpliceConstants.DEFAULT_COMPRESSION);
		String compression = SpliceConstants.DEFAULT_COMPRESSION;
		desc.addFamily(new HColumnDescriptor(SpliceConstants.DEFAULT_FAMILY.getBytes(),
				SpliceConstants.DEFAULT_VERSIONS,
				compression,
				SpliceConstants.DEFAULT_IN_MEMORY,
				SpliceConstants.DEFAULT_BLOCKCACHE,
				SpliceConstants.DEFAULT_TTL,
				SpliceConstants.DEFAULT_BLOOMFILTER));
		admin.createTable(desc);
		return new HTable(admin.getConfiguration(), tableName);
	}
}
