/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
