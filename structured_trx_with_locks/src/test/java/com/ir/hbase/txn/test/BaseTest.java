package com.ir.hbase.txn.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.ir.hbase.txn.ZkTransactionManager;

public class BaseTest extends TestConstants {
    
	@SuppressWarnings("deprecation")
	@BeforeClass
	public static void setUp() throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.property.clientPort", "21818");
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.region.classes", "com.ir.hbase.txn.coprocessor.region.TransactionalManagerRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalRegionObserver");
		hbaseTestingUtility = new HBaseTestingUtility(conf);
		hbaseTestingUtility.startMiniCluster();
		admin = new HBaseAdmin(hbaseTestingUtility.getConfiguration());		
		zkw = admin.getConnection().getZooKeeperWatcher();
		rzk = zkw.getRecoverableZooKeeper();
		tm = new ZkTransactionManager(hbaseTestingUtility.getConfiguration());
	}
	
	@AfterClass
	public static void cleanUp() throws Exception {
		hbaseTestingUtility.cleanupTestDir();
		hbaseTestingUtility.shutdownMiniCluster();
	}
	
}
