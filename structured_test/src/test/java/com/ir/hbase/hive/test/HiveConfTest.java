package com.ir.hbase.hive.test;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import com.ir.constants.HBaseConstants;
import com.ir.hbase.hive.HBaseSerDe;

public class HiveConfTest {

	@Test 
	public void test () {
		System.out.println((new JobConf(HBaseConstants.class)).getJar());
		
	}
	
}
