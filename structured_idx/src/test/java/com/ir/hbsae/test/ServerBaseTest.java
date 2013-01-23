package com.ir.hbsae.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.junit.BeforeClass;
import com.ir.hbase.client.SchemaManager;
import com.ir.hbase.index.test.BaseTest;

import com.ir.hbase.txn.TerminalTransactionManager;

public class ServerBaseTest extends BaseTest {
	protected static Configuration conf;
	protected static HBaseAdmin admin;
	protected static SchemaManager sm;
	protected static RecoverableZooKeeper rzk;
	protected static TerminalTransactionManager tm;
	
	@BeforeClass
	public static void setUp() throws Exception {
		conf = HBaseConfiguration.create();
		admin = new HBaseAdmin(conf);
		sm = new SchemaManager(admin);
		tm = new TerminalTransactionManager(conf);
		rzk = admin.getConnection().getZooKeeperWatcher().getRecoverableZooKeeper();
	}
}
