package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Created by jyuan on 11/11/14.
 */
public class SpliceSequenceKey extends AbstractSequenceKey{

    public SpliceSequenceKey(
                             byte[] sysColumnsRow,
                             long autoIncStart,
                             long autoIncrement,
                             long blockAllocationSize) {
        super(sysColumnsRow, blockAllocationSize,autoIncStart,autoIncrement);
    }

    public SpliceSequence makeNew() throws StandardException {
        return new SpliceSequence(blockAllocationSize,sysColumnsRow,
                autoIncStart, autoIncrement);
    }
}
