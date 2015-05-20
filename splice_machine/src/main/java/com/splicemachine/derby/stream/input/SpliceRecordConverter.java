package com.splicemachine.derby.stream.input;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;

public class SpliceRecordConverter extends RecordMaterializer<ExecRow> {

    public SpliceRecordConverter() {

    }

    @Override
    public ExecRow getCurrentRecord() {
        return null;
    }

    @Override
    public GroupConverter getRootConverter() {
        return null;
    }

}