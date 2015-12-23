package com.splicemachine.pipeline.api;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface BulkWriterFactory{

    BulkWriter newWriter(byte[] tableName);

    void invalidateCache(byte[] tableName) throws IOException;
}
