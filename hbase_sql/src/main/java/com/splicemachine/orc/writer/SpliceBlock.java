package com.splicemachine.orc.writer;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.orc.block.BlockFactory;
import com.splicemachine.orc.block.ColumnBlock;
import com.splicemachine.utils.IntArrays;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * Splice Machine's Data Block Page.
 *
 */
public class SpliceBlock {
    private ExecRow execRow;
    ColumnBlock[] columnBlocks;
    private static int BLOCK_SIZE = 10000;
    private int positionCount;
    public SpliceBlock(ExecRow execRow) {
        assert execRow!=null:"ExecRow Definition Must Exist";
        this.execRow = execRow;
        columnBlocks = new ColumnBlock[execRow.size()];
        StructType structType = execRow.createStructType(IntArrays.count(execRow.size()));
        StructField[] fields = structType.fields();
        for (int i = 0; i< fields.length; i++) {
            DataType dataType = fields[i].dataType();
            columnBlocks[i] = BlockFactory.getColumnBlock(ColumnVector.allocate(BLOCK_SIZE,dataType, MemoryMode.ON_HEAP),dataType);
        }
    }

    public void addExecRow(ExecRow execRow) throws StandardException {
        for (int i = 0; i< columnBlocks.length; i++) {
            columnBlocks[i].setValue(execRow.getColumn(i+1));
        }
        positionCount++;
    }

    public void reset() {
        positionCount = 0;

    }

    public int getPositionCount() {
        return positionCount;
    }

    public int getChannelCount() {
        return columnBlocks.length;
    }

    public ColumnBlock getColumnBlock(int channel) {
        return columnBlocks[channel];
    }

}
