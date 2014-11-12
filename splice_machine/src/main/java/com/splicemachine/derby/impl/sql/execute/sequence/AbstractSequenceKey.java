package com.splicemachine.derby.impl.sql.execute.sequence;

import java.util.Arrays;
import org.apache.derby.iapi.error.StandardException;
import com.splicemachine.tools.ResourcePool;

public abstract class AbstractSequenceKey implements ResourcePool.Key{
        protected final byte[] sysColumnsRow;
        protected final long blockAllocationSize;
        protected long autoIncStart;
        protected long autoIncrement;
        public AbstractSequenceKey(
                   byte[] sysColumnsRow,
                   long blockAllocationSize) {
            this.sysColumnsRow = sysColumnsRow;
            this.blockAllocationSize = blockAllocationSize;
        }

        public byte[] getSysColumnsRow(){
            return sysColumnsRow;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof AbstractSequenceKey)) return false;
            AbstractSequenceKey key = (AbstractSequenceKey) o;
            return Arrays.equals(sysColumnsRow, key.sysColumnsRow) && blockAllocationSize == key.blockAllocationSize;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(sysColumnsRow);
        }

        public long getStartingValue() throws StandardException{
            getStartAndIncrementFromSystemTables();
            if(autoIncStart<=0l) return 0l;
            return autoIncStart;
        }

        public long getIncrementSize() throws StandardException{
            getStartAndIncrementFromSystemTables();
            if(autoIncrement<=0l) return 1l;
            return autoIncrement;
        }

        protected abstract void getStartAndIncrementFromSystemTables() throws StandardException;
        
    }