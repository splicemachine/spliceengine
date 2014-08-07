package com.splicemachine.si.api;

import com.splicemachine.utils.ByteSlice;

/**
 * Interface for notifying other systems that a read was resolved.
 *
 * Read resolution is the act of ensuring that a particular version of a particular
 * row has the correct metadata attached to it when its writing transaction is either
 * committed or rolled back.
 *
 * When a writing transaction is rolled back, Read Resolution should delete the physical representation
 * of the data so as to avoid the IO cost of reading junk data.
 *
 * When a writing transaction is committed, Read Resolution should attach a commit timestamp
 * to the data to avoid repeated transaction lookups for future reads.
 *
 * @author Scott Fines
 * Date: 6/25/14
 */
public interface ReadResolver {

    /**
     * Mark this version of this row as resolved, either committed or rolled back.
     *
     * The implementation will make additional distinction as to whether or not a particular
     * entry can actually be read-resolved, but it still makes sense to only call this
     * when the caller knows that the transactional state is in some way final (it may not know
     * that the transaction has been fully committed all the way up the hierarchy, for example,
     * but may know that the transaction itself has been committed).
     *
     * @param rowKey the row key of the row to be resolved
     * @param txnId the transaction id (version) of the row to resolve.
     */
    void resolve(ByteSlice rowKey, long txnId);

}
