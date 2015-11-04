package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.parquet.hadoop.ParquetOutputFormat;

/**
 * Created by jleach on 5/15/15.
 */
public class SpliceParquetOutputFormat extends ParquetOutputFormat<ExecRow> {

    public SpliceParquetOutputFormat() {
        super(new SpliceWriteSupport());
    }

}
