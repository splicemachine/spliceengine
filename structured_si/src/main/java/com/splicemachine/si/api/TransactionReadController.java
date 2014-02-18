package com.splicemachine.si.api;

import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * Manages lifecycle and setup for Transactional reads.
 *
 * @author Scott Fines
 * Date: 2/13/14
 */
public interface TransactionReadController<Get,Scan,Data,Hashable extends Comparable,Result,KeyValue> {

		/**
		 * Look at the operation and report back whether it has been flagged for SI treatment.
		 */
		boolean isFilterNeededGet(Get get);
		boolean isFilterNeededScan(Scan scan);

		/**
		 * Determine whether an operation has been marked to indicate that it should return an SI column.
		 */
		boolean isGetIncludeSIColumn(Get get);
		boolean isScanIncludeSIColumn(Scan scan);

		/**
		 * Perform server-side pre-processing of operations. This is before they are actually executed.
		 */
		void preProcessGet(Get get) throws IOException;
		void preProcessScan(Scan scan) throws IOException;

		/**
		 * Construct an object to track the stateful aspects of a scan. Each key value from the scan will be considered
		 * in light of this state.
		 */
		IFilterState newFilterState(TransactionId transactionId) throws IOException;
		IFilterState newFilterState(RollForwardQueue<Data, Hashable> rollForwardQueue, TransactionId transactionId,
																boolean includeSIColumn) throws IOException;
		IFilterState newFilterStatePacked(String tableName, RollForwardQueue<Data, Hashable> rollForwardQueue,
																			EntryPredicateFilter predicateFilter, TransactionId transactionId,
																			boolean includeSIColumn) throws IOException;

		/**
		 * Consider whether to use a key value in light of a given filterState.
		 */
		Filter.ReturnCode filterKeyValue(IFilterState filterState, KeyValue keyValue) throws IOException;

		/**
		 * Indicate that the filterState is now going to be used to process a new row.
		 */
		void filterNextRow(IFilterState filterState);

		/**
		 * This is for use in code outside of a proper HBase filter that wants to apply the equivalent of SI filter logic.
		 * Pass in an entire result object that contains all of the key values for a row. Receive back a new result that only
		 * contains the key values that should be visible in light of the filterState.
		 */
		Result filterResult(IFilterState<KeyValue> filterState, Result result) throws IOException;

	  /**
     * Create a DDLFilter for tracking the visibility of (tentative) DDL operations for DML operations
     * @param transactionId Transaction ID which identifies the DDL change
     * @return Object that tracks visibility
     */
    DDLFilter newDDLFilter(String transactionId) throws IOException;
}
