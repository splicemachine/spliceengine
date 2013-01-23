package com.ir.hbase.hive.test;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.exec.Task;

import com.ir.constants.SchemaConstants;
import com.ir.constants.TxnConstants;

public class HiveClient {
	public static final String SCHEMA_PATH = "/schema";
	public static final String TRANSACTION_PATH = "/transaction";
	public static final String METASTORE_PORT = "9083";
	public static HiveMetaStoreClient client;
	public static HiveConf hiveConf;
	public static Warehouse warehouse;
	public static boolean isThriftClient = false;

	public HiveClient() {
		hiveConf = new HiveConf(Task.class);
		hiveConf.set("fs.default.name", "hdfs://Johns-MacBook-Pro.local:8020");
		hiveConf.set("hive.metastore.warehouse.dir", "/hive");
		hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + METASTORE_PORT);
		hiveConf.setBoolVar(HiveConf.ConfVars.HIVESESSIONSILENT, false);
		hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTRETRIES, 3);
		hiveConf.setIntVar(ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, 2);
		hiveConf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT,2);
		hiveConf.setBoolVar(ConfVars.METASTORE_MODE, true);
//		hiveConf.setBoolVar(HiveConf.ConfVars.LOCALMODEAUTO,true);		
		hiveConf.set("javax.jdo.PersistenceManagerFactoryClass","org.datanucleus.jdo.JDOPersistenceManagerFactory");
		try {
			client = new HiveMetaStoreClient(hiveConf);
			warehouse = new Warehouse(hiveConf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public HiveConf getHiveConf() {
		return hiveConf;
	}
	
	public HiveMetaStoreClient getHiveMetaStoreClient() {
		return client;
	}
}
