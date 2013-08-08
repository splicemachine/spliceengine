package com.splicemachine.hbase;

import com.splicemachine.hbase.writer.*;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;
import java.util.Collection;

/**
 * Protocol for implementing Batch mutations as coprocessor execs.
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public interface BatchProtocol extends CoprocessorProtocol {

    /**
     * Apply all the Mutations in a single, synchronous, bulk operation.
     *
     * @param mutationsToApply the mutations to apply
     * @throws IOException if something goes wrong applying the mutation
     */
    public MutationResponse batchMutate(MutationRequest mutationsToApply) throws IOException;

    /**
     * Delete the first row that appears after the specified rowKey, but *only* if it
     * falls before the limit key. If {@code null} is specified for the limit key,
     * then the first row found at or after the specified rowKey will be deleted.
     *
     *
     * @param transactionId
     * @param rowKey the start to search
     * @throws IOException if something goes wrong
     */
    public MutationResult deleteFirstAfter(String transactionId, byte[] rowKey, byte[] limit) throws IOException;

    BulkWriteResult bulkWrite(BulkWrite bulkWrite);
}
