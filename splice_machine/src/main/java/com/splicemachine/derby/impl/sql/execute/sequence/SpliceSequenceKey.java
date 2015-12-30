package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.api.data.TxnOperationFactory;

/**
 *
 * Created by jyuan on 11/11/14.
 */
public class SpliceSequenceKey extends AbstractSequenceKey{
    private final PartitionFactory partitionFactory;
    private final TxnOperationFactory opFactory;
    public SpliceSequenceKey(
                             byte[] sysColumnsRow,
                             long autoIncStart,
                             long autoIncrement,
                             long blockAllocationSize,
                             TxnOperationFactory operationFactory,
                             PartitionFactory partitionFactory) {
        super(sysColumnsRow, blockAllocationSize,autoIncStart,autoIncrement);
        this.partitionFactory = partitionFactory;
        this.opFactory = operationFactory;
    }

    public SpliceSequence makeNew() throws StandardException {
        return new SpliceSequence(blockAllocationSize,sysColumnsRow,
                autoIncStart, autoIncrement,partitionFactory,opFactory);
    }
}
