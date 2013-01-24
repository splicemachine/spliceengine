package com.splicemachine.hbase.txn.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import com.splicemachine.hbase.txn.ZkTransactionManager;

public class BaseTest extends TestConstants {
    
	@BeforeClass
	@SuppressWarnings(value = "deprecation")
	public static void setUp() throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.property.clientPort", "21818");
		conf.set("hbase.coprocessor.region.classes", "com.splicemachine.hbase.txn.coprocessor.region.TransactionalManagerRegionObserver,com.splicemachine.hbase.txn.coprocessor.region.TransactionalRegionObserver");		
		hbaseTestingUtility = new HBaseTestingUtility(conf);
		hbaseTestingUtility.startMiniCluster();
		admin = new HBaseAdmin(hbaseTestingUtility.getConfiguration());		
		zkw = admin.getConnection().getZooKeeperWatcher();
		rzk = zkw.getRecoverableZooKeeper();
		tm = new ZkTransactionManager(hbaseTestingUtility.getConfiguration(), zkw, rzk);
	}
	
	@AfterClass
	public static void cleanUp() throws Exception {
		hbaseTestingUtility.cleanupTestDir();
		hbaseTestingUtility.shutdownMiniCluster();
	}
	
}
