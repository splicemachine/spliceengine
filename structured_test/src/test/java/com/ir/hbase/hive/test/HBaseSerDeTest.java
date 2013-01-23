package com.ir.hbase.hive.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseSerDeTest {
	IRHiveHBaseTestingUtility hiveTestingUtility;
	@Before
	public void before() throws Exception {
		hiveTestingUtility = new IRHiveHBaseTestingUtility();
		hiveTestingUtility.start();
	}

	@After
	public void after() throws Exception {
		hiveTestingUtility.stop();
	}

	
	@Test
	public void testDeserializeWritable() {
		
	}
	
	@Test
	public void testSerialize() {
		
	}
	
	
}
