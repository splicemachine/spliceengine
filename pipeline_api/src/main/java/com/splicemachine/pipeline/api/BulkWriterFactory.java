package com.splicemachine.pipeline.api;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface BulkWriterFactory{

    BulkWriter newWriter();

    void invalidateCache(byte[] tableName);
}
