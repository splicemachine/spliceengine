package com.splicemachine.pipeline.api;

import com.splicemachine.pipeline.client.BulkWrites;
import com.splicemachine.pipeline.client.BulkWritesResult;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/31/14
 */
public interface BulkWriter{

    /**
     * Interface for invoking BulkWrites.  The refresh cache will trigger a refresh on the
     * underlying connection.
     */
    BulkWritesResult write(BulkWrites write,boolean refreshCache) throws IOException;
}
