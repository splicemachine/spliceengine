package com.splicemachine.si.impl.region;

import org.apache.hadoop.hbase.Cell;

import com.splicemachine.si.impl.TxnUtils;

/**
 * @author Scott Fines
 *         Date: 8/18/14
 */
public class ActiveTxnFilter extends BaseActiveTxnFilter<Cell> {
	
    public ActiveTxnFilter(long beforeTs, long afterTs, byte[] destinationTable) {
    	super(beforeTs,afterTs,destinationTable);
    }
    
    @Override
    public ReturnCode filterKeyValue(Cell kv) {
    	System.out.println("filterKeyValue " + kv);
    	return this.internalFilter(kv);
    }
    
    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
				/*
				 * Since the transaction id must necessarily be the begin timestamp in both
				 * the old and new format, we can filter transactions out based entirely on the
				 * row key here
				 */
        long txnId = TxnUtils.txnIdFromRowKey(buffer, offset, length);    	
        boolean withinRange = txnId >= afterTs && txnId <= beforeTs;
        return !withinRange;
    }  
}