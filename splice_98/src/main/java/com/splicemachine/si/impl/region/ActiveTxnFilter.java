package com.splicemachine.si.impl.region;

import org.apache.hadoop.hbase.Cell;

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
    	return this.internalFilter(kv);
    }
}