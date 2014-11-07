package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.operations.SkippingScanFilter;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import org.apache.hadoop.hbase.util.Pair;
import java.io.IOException;
import java.util.List;

/**
 * Unfortunately when using a SkippingScanFilter there can be different predicates for different rowKey ranges.
 *
 * This class takes the relevant info from SkippingScanFilter and makes it available to SITableScanner in
 * an efficient/convenient manner.
 */
class ScopedPredicates<Data> {

    private List<Pair<byte[], byte[]>> startStopKeys;
    private List<ObjectArrayList<Predicate>> predicates;
    private SDataLib dataLib;

    ScopedPredicates(SkippingScanFilter filter, SDataLib dataLib) throws IOException {
    	this.dataLib = dataLib;
        if (filter != null) {
            this.startStopKeys = filter.getStartStopKeys();
            this.predicates = Lists.newArrayList();
            for (byte[] predicate : filter.getPredicates()) {
                predicates.add(EntryPredicateFilter.fromBytes(predicate).getValuePredicates());
            }
        }
    }

    /**
     * Get predicates for the range containing the specified KeyValue.
     */
    public ObjectArrayList<Predicate> getNextPredicates(Data kv) throws IOException {

        // possible optimization here: remove old ranges/predicates less than the passed KeyValue?

        for (int i = 0; i < startStopKeys.size(); i++) {
            Pair<byte[], byte[]> range = startStopKeys.get(i);
            if (dataLib.isDataInRange(kv, range)) {
                return predicates.get(i);
            }
        }
        /* No predicates for this KeyValue, return empty predicate list. */
        ObjectArrayList<Predicate> EMPTY = EntryPredicateFilter.EMPTY_PREDICATE.getValuePredicates();
        /* Returning a mutable constant (optimization) here, assumes caller will not modify! */
        Preconditions.checkState(EMPTY.isEmpty());
        return EMPTY;
    }

    public boolean isScanWithScopedPredicates() {
        return predicates != null;
    }

}
