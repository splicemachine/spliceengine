package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;

public class SpliceIdentityColumnKey extends AbstractSequenceKey {
    private final long seqConglomId;
    private final int columnNum;
    private boolean systemTableSearched = false;


    public SpliceIdentityColumnKey(HTableInterface table,
    		byte[] sysColumnsRow,
            long seqConglomId,
            int columnNum,
            long blockAllocationSize,
            long autoIncStart,
            long autoIncrement
            ) {
    	super(table, sysColumnsRow, blockAllocationSize,autoIncStart,autoIncrement);
    	this.seqConglomId = seqConglomId;
    	this.columnNum = columnNum;
    }

    @Override
	public SpliceSequence makeNew() throws StandardException {
        return new SpliceSequence(table,
                blockAllocationSize,sysColumnsRow,
                getStartingValue(),getIncrementSize());		
	}
	
}
