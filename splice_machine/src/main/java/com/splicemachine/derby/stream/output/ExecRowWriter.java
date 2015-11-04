package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.parquet.io.api.RecordConsumer;

/**
 * Created by jleach on 5/15/15.
 */
public class ExecRowWriter {
    private final RecordConsumer recordConsumer;

    public ExecRowWriter(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }


    public void write(ExecRow execRow) throws StandardException {
        ParquetExecRowUtils.writeRow(execRow,recordConsumer);
    }
}