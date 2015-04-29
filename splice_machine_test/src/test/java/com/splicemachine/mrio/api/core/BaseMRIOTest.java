package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import org.junit.Ignore;

@Ignore("Breaks stuff")
public class BaseMRIOTest extends SpliceUnitTest {
	protected static Configuration config;
	protected static SMSQLUtil sqlUtil;
	
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
		config.set(HConstants.HBASE_DIR,getHBaseDirectory());
        config.set("fs.default.name", "file:///"); // MapR Hack, tells it local filesystem
    	config.set(MRConstants.SPLICE_JDBC_STR, SpliceNetConnection.getDefaultLocalURL());
    	System.setProperty("hive.metastore.warehouse.dir", getHiveWarehouseDirectory());
    	System.setProperty("mapred.job.tracker", "local");
    	System.setProperty("mapreduce.framework.name", "local-chicken");
    	System.setProperty(HConstants.HBASE_DIR, getHBaseDirectory());
    	System.setProperty("hive.exec.mode.local.auto","true");
    	System.setProperty("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=target/metastore_db;create=true");
		sqlUtil = SMSQLUtil.getInstance(SpliceNetConnection.getDefaultLocalURL());
	}
	
	protected static void flushTable(String tableName) throws SQLException, IOException, InterruptedException {
		String conglomId = sqlUtil.getConglomID(tableName);
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(config);
			admin.flush(conglomId);
		} finally { 
    		if (admin != null)
    			admin.close();
    		
			}
	}

	
	protected static void compactTable(String tableName) throws SQLException, IOException, InterruptedException {
		String conglomId = sqlUtil.getConglomID(tableName);
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(config);
			admin.majorCompact(conglomId);
		} finally { 
    		if (admin != null)
    			admin.close();    		
			}
	}

	protected static void splitTable(String tableName) throws SQLException, IOException, InterruptedException {
		String conglomId = sqlUtil.getConglomID(tableName);
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(config);
			admin.split(conglomId);
		} finally { 
    		if (admin != null)
    			admin.close();
 
			}
	}
	
}
