package com.splicemachine.pipeline.api;

import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.impl.BulkWritesResult;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/31/14
 */
public interface BulkWritesInvoker {

    /**
     * Interface for invoking BulkWrites.  The refresh cache will trigger a refresh on the
     * underlying connection.
     */
    BulkWritesResult invoke(BulkWrites write, boolean refreshCache) throws IOException;

    public static interface Factory {
        BulkWritesInvoker newInstance();
    }
}
