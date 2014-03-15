package com.splicemachine.si.api;

import com.splicemachine.si.impl.ConflictResults;
import com.splicemachine.si.impl.ImmutableTransaction;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Set;

/**
 * Abstraction for checking whether or not a given row is in conflict.
 *
 * This may also include things like Unique Constraint checking, etc.
 * @author Scott Fines
 * Date: 3/14/14
 */
public interface ConflictDetector<Table,Put,Lock> {

		public ConflictResults detectConflicts(ImmutableTransaction writingTxn, Result row) throws IOException;

		public void resolveConflicts(Table table, Put finalPut,Lock lock,Set<Long[]> childConflicts) throws IOException;
}
