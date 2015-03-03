package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * Created by jyuan on 11/11/14.
 */
public class SpliceSequenceKey extends AbstractSequenceKey{

    public SpliceSequenceKey(HTableInterface table,
                             byte[] sysColumnsRow,
                             long start,
                             long increment,
                             long blockAllocationSize) {
        super(table, sysColumnsRow, blockAllocationSize);
        autoIncStart = start;
        autoIncrement = increment;
    }

    protected void getStartAndIncrementFromSystemTables() throws StandardException {

    }

    public SpliceSequence makeNew() throws StandardException {
        return new SpliceSequence(table,
                blockAllocationSize,sysColumnsRow,
                autoIncStart, autoIncrement);
    }
}
