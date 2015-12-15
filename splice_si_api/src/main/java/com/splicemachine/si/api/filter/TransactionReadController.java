package com.splicemachine.si.api.filter;

import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.storage.DataGet;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.EntryPredicateFilter;

import java.io.IOException;

/**
 * Manages lifecycle and setup for Transactional reads.
 *
 * @author Scott Fines
 *         Date: 2/13/14
 */
public interface TransactionReadController<Data,Get,ReturnCode,Scan>{

    /**
     * Look at the operation and report back whether it has been flagged for SI treatment.
     */
    boolean isFilterNeededGet(Get get);

    boolean isFilterNeededScan(Scan scan);


    /**
     * Perform server-side pre-processing of operations. This is before they are actually executed.
     */
    void preProcessGet(Get get) throws IOException;

    void preProcessGet(DataGet get) throws IOException;

    void preProcessScan(Scan scan) throws IOException;

    void preProcessScan(DataScan scan) throws IOException;

    TxnFilter newFilterState(TxnView txn) throws IOException;

    TxnFilter newFilterState(ReadResolver readResolver,TxnView txn) throws IOException;

    TxnFilter newFilterStatePacked(ReadResolver readResolver,
                                   EntryPredicateFilter predicateFilter,
                                   TxnView txn,boolean countStar) throws IOException;

    /**
     * Consider whether to use a key value in light of a given filterState.
     */
    ReturnCode filterKeyValue(TxnFilter<Data, ReturnCode> filterState,Data data) throws IOException;

    /**
     * Indicate that the filterState is now going to be used to process a new row.
     */
    void filterNextRow(TxnFilter<Data, ReturnCode> filterState);

    /**
     * This is for use in code outside of a proper HBase filter that wants to apply the equivalent of SI filter logic.
     * Pass in an entire result object that contains all of the key values for a row. Receive back a new result that only
     * contains the key values that should be visible in light of the filterState.
     */
    DataResult filterResult(TxnFilter<Data, ReturnCode> filterState,DataResult result) throws IOException;

    /**
     * Create a DDLFilter for tracking the visibility of (tentative) DDL operations for DML operations
     *
     * @param txn the ddl transaction
     * @return Object that tracks visibility
     */
    DDLFilter newDDLFilter(TxnView txn) throws IOException;
}
