package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.sql.execute.ExecRow;

import java.nio.ByteBuffer;

public class DistinctMerger implements HashMerger{


    @Override
    public void merge(HashBuffer currentRows, ExecRow currentRow, ExecRow nextRow) {
    }

    @Override
    public ExecRow shouldMerge(HashBuffer currentRows, Object key) {
        return currentRows.get(key);
    }
}
