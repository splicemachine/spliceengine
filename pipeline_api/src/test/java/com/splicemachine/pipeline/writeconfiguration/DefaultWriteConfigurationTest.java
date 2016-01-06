/**
 * 
 */
package com.splicemachine.pipeline.writeconfiguration;

import java.util.concurrent.ExecutionException;

import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.config.DefaultWriteConfiguration;
import com.splicemachine.pipeline.testsetup.PipelineTestDataEnv;
import com.splicemachine.pipeline.testsetup.PipelineTestEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.splicemachine.pipeline.api.WriteResponse;

/**
 * @author David Winters
 * Created on: 5/10/15
 */
public class DefaultWriteConfigurationTest {
    private PipelineExceptionFactory exceptionFactory;

    @Before
    public void setUp() throws Exception{
        PipelineTestDataEnv testDataEnv=PipelineTestEnvironment.loadTestDataEnvironment();

        this.exceptionFactory = testDataEnv.pipelineExceptionFactory();
    }
	/**
	 * MapR likes to throw CallerDisconnectedExceptions during bulk writes and BulkWriteAction was retrying infinitely
	 * since it was always receiving this exception since the client went "bye-bye".  The import task needs to be failed in this case.
	 * @throws ExecutionException
	 */
	@Test
	public void testCallerDisconnectedException() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null,exceptionFactory);
		Assert.assertEquals(WriteResponse.THROW_ERROR, configuration.globalError(exceptionFactory.callerDisconnected("Disconnected")));
	}

	@Test
	public void testDoNotRetryIOException() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null,exceptionFactory);
		Assert.assertEquals(WriteResponse.THROW_ERROR, configuration.globalError(exceptionFactory.doNotRetry("Some I/O exception occurred")));
	}

	@Test
	public void testRpcClientFailedServerException() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null,exceptionFactory);
		Assert.assertEquals(WriteResponse.THROW_ERROR, configuration.globalError(exceptionFactory.failedServer("A server has failed")));
	}

	@Test
	public void testNotServingRegionException() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null,exceptionFactory);
		Assert.assertEquals(WriteResponse.RETRY, configuration.globalError(exceptionFactory.notServingPartition("Some remote region not serving exception occurred")));
	}
}
