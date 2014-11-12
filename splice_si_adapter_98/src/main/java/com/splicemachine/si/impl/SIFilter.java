package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.Cell;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilter extends BaseSIFilter<Cell> {
    public SIFilter() {
    	super();
    }

    public SIFilter(TxnFilter txnFilter){
    	super(txnFilter);
    }

    @Override
		@SuppressWarnings("unchecked")
    public ReturnCode filterKeyValue(Cell keyValue) {
    	return internalFilter(keyValue);
    }
}