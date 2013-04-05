package com.splicemachine.derby.error;

import org.junit.Assert;
import org.junit.Test;

public class SpliceStandardLogUtilsTest {
	public static final String ERROR_STRING = "{\"severity\":20000,\"textMessage\":\"The resulting value is outside the range for the data type DOUBLE.\",\"sqlState\":\"22003\"}";
	public static final String FULL_ERROR_STRING = "instance with messsage com.splicemachine.derby.error.SpliceDoNotRetryIOException: " + ERROR_STRING +
	"at com.splicemachine.derby.error.SpliceStandardLogUtils.generateSpliceDoNotRetryIOException(SpliceStandardLogUtils.java:13)"+
	"at com.splicemachine.derby.hbase.SpliceOperationRegionScanner.next(SpliceOperationRegionScanner.java:133)"+
	"at org.apache.hadoop.hbase.regionserver.HRegionServer.next(HRegionServer.java:2159)"+
	"at sun.reflect.GeneratedMethodAccessor21.invoke(Unknown Source)" +
	"at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)" +
	"at java.lang.reflect.Method.invoke(Method.java:597)" +
	"at org.apache.hadoop.hbase.ipc.WritableRpcEngine$Server.call(WritableRpcEngine.java:364)" +
	"at org.apache.hadoop.hbase.ipc.HBaseServer$Handler.run(HBaseServer.java:1336)";
	
	
	@Test
	public void parseMessageTest() {
		Assert.assertEquals(ERROR_STRING, SpliceStandardLogUtils.parseMessage(FULL_ERROR_STRING));
	}
}
