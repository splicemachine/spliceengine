package com.splicemachine.derby.error;

import java.io.IOException;
import org.junit.Assert;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.junit.Test;

import com.splicemachine.derby.error.SpliceIOException;

public class SpliceIOExceptionTest {
	protected static String msg1 = "CHECK WITH ME";
	protected static String msg2 = "CHECK WITH ME 2";
	protected static SpliceIOException exception = new SpliceIOException(msg1,new Exception(msg2));
	@Test
	public void instanceOfTest() {
		Assert.assertTrue(exception instanceof IOException);
		Assert.assertFalse((Throwable) exception instanceof DoNotRetryIOException);
	}
	@Test
	public void messageTrimmingTest() {
		Assert.assertEquals(msg1,exception.getMessage());
	}
	
}
