package com.splicemachine.orc.block;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.orc.writer.ColumnWriter;
import com.splicemachine.utils.IntArrays;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.OutputStream;

/**
 * Created by jleach on 9/28/17.
 */
public class SpliceDataBlock {
    private ExecRow execRow;
    ColumnBlock[] columnBlocks;
    ColumnWriter[] columnWriters;
    private static int BLOCK_SIZE = 64;
    private OutputStream outputStream;
    public SpliceDataBlock(ExecRow execRow, OutputStream outputStream) {
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

    public void addExecRow(ExecRow execRow) {


    }

    public void flush() {

    }

}
