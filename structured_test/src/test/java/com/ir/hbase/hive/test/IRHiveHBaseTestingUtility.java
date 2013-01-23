package com.ir.hbase.hive.test;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.exec.Task;

import com.ir.constants.SchemaConstants;
import com.ir.constants.TxnConstants;

public class IRHiveHBaseTestingUtility {
	public static final String SCHEMA_PATH = "/schema";
	public static final String TRANSACTION_PATH = "/transaction";
	public static final String METASTORE_PORT = "9083";
	public static HiveMetaStoreClient client;
	public static HiveConf hiveConf;
	public static Warehouse warehouse;
	public static boolean isThriftClient = false;
	public MiniHBaseCluster miniHBaseCluster;

	public static HBaseTestingUtility hbaseTestingUtility = new HBaseTestingUtility(); 
	public IRHiveHBaseTestingUtility() {
		hbaseTestingUtility = new HBaseTestingUtility();
		hbaseTestingUtility.getConfiguration().set("hbase.zookeeper.property.clientPort", "21818");
				
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.master.classes", "com.ir.hbase.coprocessor.index.IndexMasterObserver,com.ir.hbase.coprocessor.structured.StructuredMasterObserver");
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.region.classes", "com.ir.hbase.coprocessor.index.IndexRegionObserver,com.ir.hbase.coprocessor.structured.StructuredRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalManagerRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalRegionObserver");		

		
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.master.classes", "com.ir.hbase.coprocessor.index.IndexMasterObserver,com.ir.hbase.coprocessor.structured.StructuredMasterObserver");
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.region.classes", "com.ir.hbase.coprocessor.index.IndexRegionObserver,com.ir.hbase.coprocessor.structured.StructuredRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalManagerRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalRegionObserver");		
		hbaseTestingUtility.getConfiguration().set(SchemaConstants.SCHEMA_PATH_NAME, SCHEMA_PATH);
		hbaseTestingUtility.getConfiguration().set(TxnConstants.TRANSACTION_PATH_NAME, TRANSACTION_PATH);		
		hbaseTestingUtility.getConfiguration().reloadConfiguration();
		hiveConf = new HiveConf(this.getClass());
		hiveConf.set("fs.default.name", hbaseTestingUtility.getConfiguration().get("fs.default.name"));
		hiveConf.set("hive.metastore.warehouse.dir", "/hive");
		hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + METASTORE_PORT);
		hiveConf.setBoolVar(HiveConf.ConfVars.HIVESESSIONSILENT, false);
		hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTRETRIES, 3);
		hiveConf.setIntVar(ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, 2);
		hiveConf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT,2);
		hiveConf.setBoolVar(ConfVars.METASTORE_MODE, true);
		hiveConf.set(SchemaConstants.SCHEMA_PATH_NAME, SCHEMA_PATH);
		hiveConf.set(TxnConstants.TRANSACTION_PATH_NAME, TRANSACTION_PATH);	
		hiveConf.setBoolVar(HiveConf.ConfVars.LOCALMODEAUTO,true);		
		hiveConf.set("hbase.zookeeper.property.clientPort", "21818");
		hiveConf.set("javax.jdo.PersistenceManagerFactoryClass","org.datanucleus.jdo.JDOPersistenceManagerFactory");
		try {
			miniHBaseCluster = hbaseTestingUtility.startMiniCluster(1);
			client = new HiveMetaStoreClient(hiveConf);
			warehouse = new Warehouse(hiveConf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public MiniHBaseCluster start() throws Exception {
		return hbaseTestingUtility.startMiniCluster(1);
	}
	
	public void stop() throws Exception {
		hbaseTestingUtility.shutdownMiniCluster();	
	}
	
	public HiveConf getHiveConf() {
		return hiveConf;
	}
	
	public HiveMetaStoreClient getHiveMetaStoreClient() {
		return client;
	}
	
}
