package com.splicemachine.pipeline.writerejectedhandler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import com.splicemachine.pipeline.api.BufferConfiguration;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteRejectedHandler;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.impl.BulkWrites;

public class CountingHandler implements WriteRejectedHandler{
    private final WriteRejectedHandler otherWriterHandler;
    private BufferConfiguration bufferConfiguration;
    public CountingHandler(WriteRejectedHandler otherWriterHandler,BufferConfiguration bufferConfiguration)  {
        this.otherWriterHandler = otherWriterHandler;
        this.bufferConfiguration = bufferConfiguration;
    }

    @Override
    public Future<WriteStats> writeRejected(byte[] tableName, BulkWrites action, WriteConfiguration writeConfiguration) throws ExecutionException {
        bufferConfiguration.writeRejected();
        return otherWriterHandler.writeRejected(tableName,action,writeConfiguration);
    }
}


