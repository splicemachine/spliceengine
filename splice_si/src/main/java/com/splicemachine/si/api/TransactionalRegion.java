package com.splicemachine.si.api;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents a "Transactional Region", that is, a region in Hbase which is transactionally aware.
 *
 * @author Scott Fines
 *         Date: 7/1/14
 */
public interface TransactionalRegion extends AutoCloseable{

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

    DDLFilter ddlFilter(Txn ddlTxn) throws IOException;

    SICompactionState compactionFilter() throws IOException;

    /**
     * @return true if the underlying region is either closed or is closing
     */
    boolean isClosed();

		boolean rowInRange(byte[] row);

		boolean rowInRange(ByteSlice slice);

    boolean containsRange(byte[] start, byte[] stop);

    String getTableName();

    void updateWriteRequests(long writeRequests);

    void updateReadRequests(long readRequests);

    OperationStatus[] bulkWrite(TxnView txn,
                                byte[] family, byte[] qualifier,
                                ConstraintChecker constraintChecker,
                                Collection<KVPair> data) throws IOException;


    String getRegionName();

    TxnSupplier getTxnSupplier();

    ReadResolver getReadResolver();

    DataStore getDataStore();

    void close();

    InternalScanner compactionScanner(InternalScanner scanner);
}
