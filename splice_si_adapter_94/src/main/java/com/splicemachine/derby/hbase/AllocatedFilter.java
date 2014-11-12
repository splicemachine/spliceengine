package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.KeyValue;

public class AllocatedFilter extends BaseAllocatedFilter<KeyValue> {
    public AllocatedFilter() {
        super();
    }

    public AllocatedFilter(byte[] localAddress) {
        super(localAddress);
    }

	@Override
	public ReturnCode filterKeyValue(KeyValue ignored) {
		return this.internalFilter(ignored);
	}
}
