package com.splicemachine.derby.impl.load.spark;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Created by dgomezferro on 7/22/15.
 */
public class ImportResult {
    private long imported;
    private long badRecords;
    private StandardException exception;

    public ImportResult(long imported, long badRecords) {
        this.imported = imported;
        this.badRecords = badRecords;
    }

    public ImportResult(StandardException exception) {
        this.exception = exception;
    }

    public long getImported() {
        return imported;
    }

    public long getBadRecords() {
        return badRecords;
    }

    public StandardException getException() {
        return exception;
    }
}
