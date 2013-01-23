package com.ir.hbase.hive.test;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ir.constants.HBaseConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestIRHiveWithCli extends TestRemoteHiveMetaStore {
//	public static String filePath = "/Users/kefulu/Documents/workspace/cccm/cccm-hive/src/test/java/com/ir/hive/metastore/test/";
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
	public void testCreateTableWithCliDriver() throws IOException {
		ArrayList<String> cmds = new ArrayList<String>();
		try {
			cmds.add("create table firsttablea (key INT, value string) STORED BY 'com.ir.hbase.hive.HBaseStorageHandler' " +
					"WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,cf1:val\")" +
					"TBLPROPERTIES (\"hbase.table.name\" = \"firsttablea\");");
			cmds.add("show tables");	
			cmdLine(cmds);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		} 
		HTable table = new HTable(HBaseConfiguration.create(), "firsttablea");
		Put put = new Put("yo".getBytes());
		put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), "key".getBytes(), Bytes.toBytes(1));
		put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), "value".getBytes(), Bytes.toBytes("Value String?"));
		table.put(put);
		
		cmds = new ArrayList<String>();
		try {
			cmds.add("select count(1) from firsttablea");
			cmds.add("drop table firsttablea");
			cmdLine(cmds);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		} 

	
	
	
	}
	

	@Test
	public void testSelectFromTable() {
		try {
			ArrayList<String> cmds = new ArrayList<String>();
			cmds.add("USE a__b__c");
			cmds.add("select count(1) from testTable;");
			cmdLine(cmds);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	/*
	@Test
	public void testSimpleFromFile() {
		try {
			ArrayList<String> cmds = new ArrayList<String>();
			cmds.add("-i " + filePath+"createTest");
			cmdLine(cmds);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}*/
	
	/*
	@Test
	public void testConcurrentCreateDatabase() {
		try {
			String[] cmd = new String[2];
			cmd[0] = "create database b__c__d;";
			cmd[1] = "show databases;";
			concurrentCmds(cmd, 0);

			cmdLine("show databases");
			cmdLine("drop database b__c__d");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testConcurrentAlterTable() {
		String[] cmd = new String[2];
		cmd[0] = "USE a__b__c;";
		cmd[1] = "ALTER TABLE testTable ADD COLUMNS (second STRING);";
		try {
			concurrentCmds(cmd,0);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}*/
	
	/*
	protected void concurrentCmds(final String[] cmd, int number) throws Exception {
		final int maxNumb = number;
		Thread[] factory = new Thread[maxNumb];
		for (int i = 0; i < factory.length ; i++) {
			final int numb = i+1;
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() { 
					try {
						System.out.println("Thread " + numb + " of " + maxNumb);
						for (int j=0; j< cmd.length;j++) {
							if (j==0) {
								cmdLine(cmd[j]);
							} else {
								cmdLine(cmd[j]);
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
						Assert.fail(e.getMessage());
					}
				}

			});
			factory[i]=t;
			t.start();
		}
		for (int i=0; i <factory.length ; i++) {
			factory[i].join();
		}
	}*/
	
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
