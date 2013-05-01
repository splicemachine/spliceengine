package com.ir.hbase.hive.test;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;

import com.ir.constants.SpliceConstants;
import com.ir.hbase.hive.HBaseSerDe;

public class HiveConfTest {

	@Test 
	public void test () {
		Assert.assertNotNull(new JobConf(SpliceConstants.class).getJar());
		
	}
	
}
