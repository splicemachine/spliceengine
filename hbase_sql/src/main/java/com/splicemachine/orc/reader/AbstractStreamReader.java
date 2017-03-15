package com.splicemachine.orc.reader;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;

/**
 * Created by jleach on 3/14/17.
 */
public abstract class AbstractStreamReader implements StreamReader {
    protected int nextBatchSize;
    protected int readOffset;
    @Override
    public ColumnVector readBlock(DataType type)
            throws IOException {
        return readBlock(type,ColumnVector.allocate(nextBatchSize,type, MemoryMode.ON_HEAP));
    }

    @Override
    public int getBatchSize() {
        return nextBatchSize;
    }
}
