package com.splicemachine.si.impl.region;


/**
 * @author Scott Fines
 *         Date: 8/18/14
 */
class ActiveTxnFilter extends BaseActiveTxnFilter<Cell> {

    public ActiveTxnFilter(long beforeTs, long afterTs, byte[] destinationTable) {
    	super(beforeTs,afterTs,destinationTable);
    }

    @Override
    public ReturnCode filterKeyValue(Cell kv) {
    	return this.internalFilter(kv);
    }
}