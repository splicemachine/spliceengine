/**
 * 
 */
package com.splicemachine.pipeline.writeconfiguration;

import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.junit.Assert;
import org.junit.Test;

import com.splicemachine.pipeline.api.WriteResponse;

/**
 * @author David Winters
 * Created on: 5/10/15
 */
public class DefaultWriteConfigurationTest {

	/**
	 * MapR likes to throw CallerDisconnectedExceptions during bulk writes and BulkWriteAction was retrying infinitely
	 * since it was always receiving this exception since the client went "bye-bye".  The import task needs to be failed in this case.
	 * @throws ExecutionException
	 */
	@Test
	public void testCallerDisconnectedException() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null);
		Assert.assertEquals(WriteResponse.THROW_ERROR, configuration.globalError(new CallerDisconnectedException("Disconnected")));
	}

	@Test
	public void testDoNotRetryIOException() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null);
		Assert.assertEquals(WriteResponse.THROW_ERROR, configuration.globalError(new DoNotRetryIOException("Some I/O exception occurred")));
	}

	@Test
	public void testRemoteWithExtrasExceptionWithDoNotRetryIOException() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null);
		Assert.assertEquals(WriteResponse.THROW_ERROR, configuration.globalError(new RemoteWithExtrasException(DoNotRetryIOException.class.getCanonicalName(), "Some remote I/O exception occurred", true)));
	}

	@Test
	public void testRpcClientFailedServerException() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null);
		Assert.assertEquals(WriteResponse.THROW_ERROR, configuration.globalError(new RpcClient.FailedServerException("A server has failed")));
	}

	@Test
	public void testNotServingRegionException() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null);
		Assert.assertEquals(WriteResponse.RETRY, configuration.globalError(new org.apache.hadoop.hbase.NotServingRegionException("Some remote region not serving exception occurred")));
	}

	@Test
	public void testRemoteWithExtrasExceptionWithNotServingRegionExceptionDoNotRetryTrue() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null);
		Assert.assertEquals(WriteResponse.RETRY, configuration.globalError(new org.apache.hadoop.hbase.ipc.RemoteWithExtrasException(org.apache.hadoop.hbase.NotServingRegionException.class.getCanonicalName(), "Some remote region not serving exception occurred", true)));
	}

	@Test
	public void testRemoteWithExtrasExceptionWithNotServingRegionExceptionDoNotRetryFalse() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null);
		Assert.assertEquals(WriteResponse.RETRY, configuration.globalError(new org.apache.hadoop.hbase.ipc.RemoteWithExtrasException(org.apache.hadoop.hbase.NotServingRegionException.class.getCanonicalName(), "Some remote region not serving exception occurred", false)));
	}
}
