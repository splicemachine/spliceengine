package com.splicemachine.error;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.junit.Test;

public class SpliceDoNotRetryIOExceptionTest {
	protected static String msg1 = "CHECK WITH ME";
	protected static String msg2 = "CHECK WITH ME 2";
	protected static SpliceDoNotRetryIOException exception = new SpliceDoNotRetryIOException(msg1,new Exception(msg2));
	@Test
	public void instanceOfTest() {
		Assert.assertTrue(exception instanceof IOException);
		Assert.assertTrue(exception instanceof DoNotRetryIOException);
	}
	@Test
	public void messageTrimmingTest() {
		Assert.assertEquals(msg1,exception.getMessage());
	}
	
}
