package com.ir.hbase.test.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.junit.BeforeClass;
import com.ir.hbase.client.SchemaManager;

import com.ir.hbase.test.BaseTest;

public class YarnBaseTest extends BaseTest {
	protected static Configuration conf;
	protected static HBaseAdmin admin;
	protected static SchemaManager sm;
	protected static RecoverableZooKeeper rzk;

	
	@BeforeClass
	public static void setUp() throws Exception {
		conf = HBaseConfiguration.create();
		admin = new HBaseAdmin(conf);
		sm = new SchemaManager(admin);
		rzk = admin.getConnection().getZooKeeperWatcher().getRecoverableZooKeeper();
	}
}
