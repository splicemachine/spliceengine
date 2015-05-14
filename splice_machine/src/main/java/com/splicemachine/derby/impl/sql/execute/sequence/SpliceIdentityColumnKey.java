package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;

public class SpliceIdentityColumnKey extends AbstractSequenceKey {

    public SpliceIdentityColumnKey(
    		byte[] sysColumnsRow,
            long blockAllocationSize,
            long autoIncStart,
            long autoIncrement
            ) {
    	super(sysColumnsRow, blockAllocationSize,autoIncStart,autoIncrement);
    }

    @Override
	public SpliceSequence makeNew() throws StandardException {
        return new SpliceSequence(blockAllocationSize,sysColumnsRow,
                getStartingValue(),getIncrementSize());		
	}
	
}
