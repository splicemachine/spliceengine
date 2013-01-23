package com.ir.hbase.test.txn;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import com.ir.constants.TxnConstants;

public class BaseTxnTest extends TxnConstants {
	protected static HBaseTestingUtility hbaseTestingUtility = new HBaseTestingUtility(); 
	public static void basicConf() throws Exception {
		hbaseTestingUtility.getConfiguration().set(TxnConstants.TRANSACTION_PATH_NAME, DEFAULT_TRANSACTION_PATH);
		hbaseTestingUtility.getConfiguration().set("hbase.zookeeper.property.clientPort", "21818");
		hbaseTestingUtility.getConfiguration().reloadConfiguration();
	}
	
	public static void loadTxnCoprocessor() {
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.region.classes", "com.ir.hbase.txn.coprocessor.region.TransactionalManagerRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalRegionObserver");
		hbaseTestingUtility.getConfiguration().reloadConfiguration();
	}
	
	public static void start() throws Exception {
		hbaseTestingUtility.startMiniCluster(1);
	}
	public static void end() throws Exception {
		hbaseTestingUtility.shutdownMiniCluster();		
	}	
}
