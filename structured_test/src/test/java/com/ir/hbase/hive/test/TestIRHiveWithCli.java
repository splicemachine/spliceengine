package com.ir.hbase.hive.test;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.ir.constants.HBaseConstants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestIRHiveWithCli extends TestRemoteHiveMetaStore {
	public CliSessionState init;
	public CliDriver cliDriver;

	@Before
	public void setUpDefaults() throws Exception {
			init();
	}

	@Test
	public void testRunningCli() {
		try {
			ArrayList<String> cmds = new ArrayList<String>();
			cmds.add("show databases;");
			cmdLine(cmds);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		} 
	}
	
	@Test
	public void load1MRowsA() throws IOException {
		HTable table = new HTable(HBaseConfiguration.create(), "firsttablea");
		for (int i=0;i<100000;i++) {
			Put put = new Put(Bytes.toBytes(i));
			put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), "key".getBytes(), Bytes.toBytes(i));
			put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), "value".getBytes(), Bytes.toBytes("Value String" + i));
			put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), "money".getBytes(), Bytes.toBytes(new Double(i)+1.23));
			table.put(put);
		}	
	}

	@Test
	public void load1MRowsB() throws IOException {
		HTable table = new HTable(HBaseConfiguration.create(), "firsttableb");
		for (int i=0;i<100000;i++) {
			Put put = new Put(Bytes.toBytes(i));
			put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), "key".getBytes(), Bytes.toBytes(i));
			put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), "value".getBytes(), Bytes.toBytes("Value String" + i));
			put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), "money".getBytes(), Bytes.toBytes(new Double(i)+1.23));
			table.put(put);
		}	
	}

	
	
	protected void cmdLine(List<String> cmds) throws IOException {
		init = new CliSessionState(hiveConf);
		init.out = System.out;
		init.err = init.out;
		SessionState.start(init);
		cliDriver = new CliDriver();
		try {
			cliDriver.processInitFiles(init);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail("Error in useCliDriver's Init");
		}
		for (String cmd : cmds) {
			System.out.println("Running Command " + cmd);
			cliDriver.processLine(cmd);
		}
	}
	

	@After
	public void clean() {
		//cleanUp();
	}
}