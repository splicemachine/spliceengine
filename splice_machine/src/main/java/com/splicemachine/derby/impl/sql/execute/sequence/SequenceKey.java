package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.storage.Partition;
import com.splicemachine.tools.ResourcePool;

import java.util.Arrays;

public class SequenceKey implements ResourcePool.Key{
    private final PartitionFactory partitionFactory;
    private final TxnOperationFactory opFactory;
    protected final byte[] sysColumnsRow;
    protected final long blockAllocationSize;
    protected long autoIncStart;
    protected long autoIncrement;

    public SequenceKey(
            byte[] sysColumnsRow,
            long blockAllocationSize,
            long autoIncStart,
            long autoIncrement,
            PartitionFactory partitionFactory,
            TxnOperationFactory opFactory){
        assert (sysColumnsRow!=null):"Null SysColumnsRow Passed in";
        this.partitionFactory = partitionFactory;
        this.opFactory = opFactory;
        this.sysColumnsRow=sysColumnsRow;
        this.blockAllocationSize=blockAllocationSize;
        this.autoIncStart=autoIncStart;
        this.autoIncrement=autoIncrement;
    }

    public byte[] getSysColumnsRow(){
        return sysColumnsRow;
    }

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof SequenceKey)) return false;
        SequenceKey key=(SequenceKey)o;
        return Arrays.equals(sysColumnsRow,key.sysColumnsRow)
                && blockAllocationSize==key.blockAllocationSize &&
                autoIncStart==key.autoIncStart &&
                autoIncrement==key.autoIncrement;
    }

    @Override
    public int hashCode(){
        return Arrays.hashCode(sysColumnsRow);
    }

    public long getStartingValue() throws StandardException{
        return autoIncStart;
    }

    public long getIncrementSize() throws StandardException{
        return autoIncrement;
    }

    public SpliceSequence makeNew() throws StandardException{
        return new SpliceSequence(blockAllocationSize,sysColumnsRow,
                autoIncStart, autoIncrement,partitionFactory,opFactory);
    }
}