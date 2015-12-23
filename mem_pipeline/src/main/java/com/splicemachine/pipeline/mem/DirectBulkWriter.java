package com.splicemachine.pipeline.mem;

import com.splicemachine.pipeline.PipelineWriter;
import com.splicemachine.pipeline.api.BulkWriter;
import com.splicemachine.pipeline.client.BulkWrites;
import com.splicemachine.pipeline.client.BulkWritesResult;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class DirectBulkWriter implements BulkWriter{
    PipelineWriter writer;

    public DirectBulkWriter(PipelineWriter writer){
        this.writer = writer;
    }

    @Override
    public BulkWritesResult write(BulkWrites write,boolean refreshCache) throws IOException{
        return writer.bulkWrite(write);
    }
}
