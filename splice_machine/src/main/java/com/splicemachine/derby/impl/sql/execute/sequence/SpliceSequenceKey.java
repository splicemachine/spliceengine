package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * Created by jyuan on 11/11/14.
 */
public class SpliceSequenceKey extends AbstractSequenceKey{

    public SpliceSequenceKey(HTableInterface table,
                             byte[] sysColumnsRow,
                             long autoIncStart,
                             long autoIncrement,
                             long blockAllocationSize) {
        super(table, sysColumnsRow, blockAllocationSize,autoIncStart,autoIncrement);
    }

    public SpliceSequence makeNew() throws StandardException {
        return new SpliceSequence(table,
                blockAllocationSize,sysColumnsRow,
                autoIncStart, autoIncrement);
    }
}
