package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;

/**
 * Created by dgomezferro on 4/14/15.
 */
public class SparkRow {
    private RowLocation rowLocation;
    private ExecRow row;

    public SparkRow(RowLocation rowLocation, ExecRow row) {
        this.rowLocation = rowLocation;
        this.row = row;
    }

    public SparkRow(ExecRow row) {
        this(null, row);
    }

    public RowLocation getRowLocation() {
        return rowLocation;
    }

    public ExecRow getRow() {
        return row;
    }

    public void setRow(ExecRow row) {
        this.row = row;
    }

    public void setRowLocation(RowLocation rowLocation) {
        this.rowLocation = rowLocation;
    }
}
