package com.splicemachine.hase.debug;

import org.apache.hadoop.hbase.KeyValue;
import com.splicemachine.hbase.debug.AbstractHBaseEntryPredicateFilter;
import com.splicemachine.storage.EntryPredicateFilter;

public class HBaseEntryPredicateFilter extends AbstractHBaseEntryPredicateFilter<KeyValue> {

	public HBaseEntryPredicateFilter(EntryPredicateFilter epf) {
		super(epf);
    }

    @Override
	public ReturnCode filterKeyValue(KeyValue ignored) {
		return internalFilter(ignored);
	}
}
