package com.splicemachine.hbase.lease;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

public class DellTest {

	@Test
	public void testRegions() throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set(HConstants.ZOOKEEPER_QUORUM, "dellaster1-hadoop-data2");
		config.set("hbase.master", "dellaster1-hadoop-master:60000");
		HBaseAdmin admin = new HBaseAdmin(config);
		admin.listTables();
	}
	
}
