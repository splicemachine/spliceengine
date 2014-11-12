package com.splicemachine.si.impl;

import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilter extends BaseSIFilter<KeyValue> {
    public SIFilter() {
    	super();
    }

    public SIFilter(TxnFilter txnFilter){
    	super(txnFilter);
    }

    @Override
		@SuppressWarnings("unchecked")
    public ReturnCode filterKeyValue(KeyValue keyValue) {
    	return internalFilter(keyValue);
    }

}
