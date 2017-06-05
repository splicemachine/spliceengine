/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

/**
 * 
 */
package com.splicemachine.pipeline.writeconfiguration;

import java.util.concurrent.ExecutionException;

import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.config.DefaultWriteConfiguration;
import com.splicemachine.pipeline.testsetup.PipelineTestDataEnv;
import com.splicemachine.pipeline.testsetup.PipelineTestEnvironment;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.splicemachine.pipeline.api.WriteResponse;
import org.junit.experimental.categories.Category;

/**
 * @author David Winters
 * Created on: 5/10/15
 */
@Category(ArchitectureSpecific.class)
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

	@Test
	public void testConnectionClosedException() throws ExecutionException {
		DefaultWriteConfiguration configuration = new DefaultWriteConfiguration(null,exceptionFactory);
		Assert.assertEquals(WriteResponse.RETRY, configuration.globalError(exceptionFactory.connectionClosingException()));
	}

}
