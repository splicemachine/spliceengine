package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.tools.ResourcePool;

import java.util.Arrays;

public abstract class AbstractSequenceKey implements ResourcePool.Key{
    protected final byte[] sysColumnsRow;
    protected final long blockAllocationSize;
    protected long autoIncStart;
    protected long autoIncrement;

    public AbstractSequenceKey(
            byte[] sysColumnsRow,
            long blockAllocationSize,
            long autoIncStart,
            long autoIncrement){
        assert (sysColumnsRow!=null):"Null SysColumnsRow Passed in";
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
        if(!(o instanceof AbstractSequenceKey)) return false;
        AbstractSequenceKey key=(AbstractSequenceKey)o;
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

    public abstract SpliceSequence makeNew() throws StandardException;
}