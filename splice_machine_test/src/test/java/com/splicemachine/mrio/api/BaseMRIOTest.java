package com.splicemachine.mrio.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceUnitTest;

public class BaseMRIOTest extends SpliceUnitTest {
	protected static Configuration config;
	protected static SMSQLUtil sqlUtil;
	
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
		config.set(HConstants.HBASE_DIR,getHBaseDirectory());
        config.set("fs.default.name", "file:///"); // MapR Hack, tells it local filesystem
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
