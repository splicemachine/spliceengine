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
public interface TransactionReadController<Get,Scan>{

    /**
     * Perform server-side pre-processing of operations. This is before they are actually executed.
     */
    void preProcessGet(Get get) throws IOException;

    void preProcessGet(DataGet get) throws IOException;

    void preProcessScan(Scan scan) throws IOException;

    void preProcessScan(DataScan scan) throws IOException;

    TxnFilter newFilterState(ReadResolver readResolver,TxnView txn) throws IOException;

    TxnFilter newFilterStatePacked(ReadResolver readResolver,
                                   EntryPredicateFilter predicateFilter,
                                   TxnView txn,boolean countStar) throws IOException;

    /**
     * Create a DDLFilter for tracking the visibility of (tentative) DDL operations for DML operations
     *
     * @param txn the ddl transaction
     * @return Object that tracks visibility
     */
    DDLFilter newDDLFilter(TxnView txn) throws IOException;
}
