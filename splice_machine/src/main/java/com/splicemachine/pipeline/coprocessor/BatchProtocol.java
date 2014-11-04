package com.splicemachine.pipeline.coprocessor;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import java.io.IOException;

/**
 * Protocol for implementing Batch mutations as coprocessor execs.
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public interface BatchProtocol extends CoprocessorProtocol {

    /**
     * Apply all the Mutations to N regions in a single, synchronous, N bulk operations.
     *
     * @param bulkWrites the mutations to apply
     * @throws IOException if something goes wrong applying the mutation
     */
    byte[] bulkWrites(byte[] bulkWrites) throws IOException;

}
