package com.ir.hbase.txn.test;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ZkBaseTest extends TestConstants {

	@BeforeClass
	public static void setUp() throws Exception {
		hbaseTestingUtility.startMiniZKCluster();
		zkw = HBaseTestingUtility.getZooKeeperWatcher(hbaseTestingUtility);
		rzk = zkw.getRecoverableZooKeeper();
	}
	
	@AfterClass
	public static void stop() throws Exception {
		hbaseTestingUtility.shutdownMiniZKCluster();
	}
}
