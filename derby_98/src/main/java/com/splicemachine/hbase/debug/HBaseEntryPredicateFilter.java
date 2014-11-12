package com.splicemachine.hbase.debug;

import org.apache.hadoop.hbase.Cell;
import com.splicemachine.hbase.debug.AbstractHBaseEntryPredicateFilter;
import com.splicemachine.storage.EntryPredicateFilter;

public class HBaseEntryPredicateFilter extends AbstractHBaseEntryPredicateFilter<Cell> {

	public HBaseEntryPredicateFilter(EntryPredicateFilter epf) {
		super(epf);
    }

    @Override
	public ReturnCode filterKeyValue(Cell ignored) {
		return internalFilter(ignored);
	}
}
