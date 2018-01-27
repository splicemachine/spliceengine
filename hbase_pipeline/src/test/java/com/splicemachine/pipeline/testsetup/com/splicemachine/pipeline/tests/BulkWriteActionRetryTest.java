package com.splicemachine.pipeline.testsetup.com.splicemachine.pipeline.tests;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.Monitor;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.config.DefaultWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.testsetup.PipelineTestDataEnv;
import com.splicemachine.pipeline.testsetup.PipelineTestEnvironment;
import org.apache.hadoop.hbase.ipc.FailedServerException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.NoRouteToHostException;

/**
 * Created by jyuan on 1/26/18.
 */
public class BulkWriteActionRetryTest {

    private PipelineExceptionFactory pef;

    @Before
    public void setUp() throws Exception{
        PipelineTestDataEnv pipelineTestDataEnv = PipelineTestEnvironment.loadTestDataEnvironment();

        this.pef = pipelineTestDataEnv.pipelineExceptionFactory();
    }

    @Test
    public void testRetryNoRouteToHostException() throws Throwable {
        WriteConfiguration config = new DefaultWriteConfiguration(new Monitor(0,0,10,10L,0),pef);
        WriteResult writeResult = new WriteResult(Code.FAILED, "NoRouteToHostException:No route to host");
        BulkWriteResult bulkWriteResult = new BulkWriteResult(writeResult);
        WriteResponse response = config.processGlobalResult(bulkWriteResult);
        Assert.assertEquals(WriteResponse.RETRY, response);

        writeResult = new WriteResult(Code.FAILED, "FailedServerException:This server is in the failed servers list");
        bulkWriteResult = new BulkWriteResult(writeResult);
        response = config.processGlobalResult(bulkWriteResult);
        Assert.assertEquals(WriteResponse.RETRY, response);

        writeResult = new WriteResult(Code.FAILED, "ServerNotRunningYetException");
        bulkWriteResult = new BulkWriteResult(writeResult);
        response = config.processGlobalResult(bulkWriteResult);
        Assert.assertEquals(WriteResponse.RETRY, response);

        writeResult = new WriteResult(Code.FAILED, "ConnectTimeoutException");
        bulkWriteResult = new BulkWriteResult(writeResult);
        response = config.processGlobalResult(bulkWriteResult);
        Assert.assertEquals(WriteResponse.RETRY, response);

        writeResult = new WriteResult(Code.PARTIAL);
        IntObjectOpenHashMap<WriteResult> failedRows = new IntObjectOpenHashMap<>();
        failedRows.put(1, new WriteResult(Code.FAILED, "NoRouteToHostException:No route to host"));
        bulkWriteResult = new BulkWriteResult(writeResult, new IntOpenHashSet(), failedRows);
        response = config.partialFailure(bulkWriteResult, null);
        Assert.assertEquals(WriteResponse.RETRY, response);


        writeResult = new WriteResult(Code.PARTIAL);
        failedRows = new IntObjectOpenHashMap<>();
        failedRows.put(1, new WriteResult(Code.FAILED, "FailedServerException:This server is in the failed servers list"));
        bulkWriteResult = new BulkWriteResult(writeResult, new IntOpenHashSet(), failedRows);
        response = config.partialFailure(bulkWriteResult, null);
        Assert.assertEquals(WriteResponse.RETRY, response);

        writeResult = new WriteResult(Code.PARTIAL);
        failedRows = new IntObjectOpenHashMap<>();
        failedRows.put(1, new WriteResult(Code.FAILED, "ServerNotRunningYetException"));
        bulkWriteResult = new BulkWriteResult(writeResult, new IntOpenHashSet(), failedRows);
        response = config.partialFailure(bulkWriteResult, null);
        Assert.assertEquals(WriteResponse.RETRY, response);

        writeResult = new WriteResult(Code.PARTIAL);
        failedRows = new IntObjectOpenHashMap<>();
        failedRows.put(1, new WriteResult(Code.FAILED, "ConnectTimeoutException"));
        bulkWriteResult = new BulkWriteResult(writeResult, new IntOpenHashSet(), failedRows);
        response = config.partialFailure(bulkWriteResult, null);
        Assert.assertEquals(WriteResponse.RETRY, response);


        NoRouteToHostException nrthe = new NoRouteToHostException();
        response = config.globalError(nrthe);
        Assert.assertEquals(WriteResponse.RETRY, response);

        FailedServerException failedServerException = new FailedServerException("Failed server");
        response = config.globalError(failedServerException);
        Assert.assertEquals(WriteResponse.RETRY, response);

        ServerNotRunningYetException serverNotRunningYetException = new ServerNotRunningYetException("Server not running");
        response = config.globalError(serverNotRunningYetException);
        Assert.assertEquals(WriteResponse.RETRY, response);

        ConnectTimeoutException connectTimeoutException = new ConnectTimeoutException("connect timeout");
        response = config.globalError(connectTimeoutException);
        Assert.assertEquals(WriteResponse.RETRY, response);
    }
}
