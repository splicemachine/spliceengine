package com.splicemachine.pipeline.writerejectedhandler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteRejectedHandler;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.impl.BulkWrites;

/**
 * WriteRejectedHandler which delegates to an additional writer (typically a synchronous writer, but perhaps a backup pool).
 *
 * Don't ever pass the same writer to this as you pass to the RegulatedWriter, or it pretty much defeats the purpose
 * of using a regulated writer.
 */
public class OtherWriteHandler implements WriteRejectedHandler{
    private final Writer otherWriter;
    
    public OtherWriteHandler(Writer otherWriter) {
        this.otherWriter = otherWriter;
    }


    @Override
    public Future<WriteStats> writeRejected(byte[] tableName, BulkWrites action, WriteConfiguration writeConfiguration) throws ExecutionException {
        return otherWriter.write(tableName,action,writeConfiguration);
    }
}
