package com.splicemachine.si.api.server;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents a "Transactional Region", that is, a region in Hbase which is transactionally aware.
 *
 * @author Scott Fines
 *         Date: 7/1/14
 */
public interface TransactionalRegion<InternalScanner> extends AutoCloseable{

    /**
     * Create a new Transactional Filter for the region.
     *
     * This filter is "Unpacked", in the sense that it will not attempt to deal with packed
     * data.
     *
     * @param txn the transaction to create a filter for
     * @return a new transactional filter for the region
     * @throws IOException if something goes wrong.
     */
    TxnFilter unpackedFilter(TxnView txn) throws IOException;

    TxnFilter packedFilter(TxnView txn, EntryPredicateFilter predicateFilter, boolean countStar) throws IOException;

    //    SICompactionState compactionFilter() throws IOException;

    /**
     * @return true if the underlying region is either closed or is closing
     */
    boolean isClosed();

    boolean rowInRange(byte[] row);

    boolean rowInRange(ByteSlice slice);

    String getTableName();

    void updateWriteRequests(long writeRequests);

    Iterable<MutationStatus> bulkWrite(TxnView txn,
                                byte[] family, byte[] qualifier,
                                ConstraintChecker constraintChecker,
                                Collection<KVPair> data) throws IOException;

    /**
     * Check for rowKey existence and update the FK row counter with given transaction's ID while holding
     * the row lock. Throw WriteConflict if referenced row has been concurrently deleted.
     *
     * @return true if the row exists.
     */
    boolean verifyForeignKeyReferenceExists(TxnView txnView, byte[] rowKey) throws IOException;

    String getRegionName();

    TxnSupplier getTxnSupplier();

    ReadResolver getReadResolver();

    void close();

    InternalScanner compactionScanner(InternalScanner scanner);

    Partition unwrap();
}
