package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardIterators;

import java.io.IOException;

/*
 * An ExecRow iterator that will skip rows which, for updates, would be NO-OP.
 *
 * DB-2007 occurs because we attempt to write an update where we change a value from NULL to NULL, and it explodes.
 * A fix for that is to check for situations where we change from NULL to NULL and not write rows that do that.
 *
 * More generally, any time a row is not changed by the update, we don't need to write that row--there's no point. This
 * block of code simply filters out rows where the updated value is the same as the original value. This has the added
 * side effect of improving write performance (fewer index changes, fewer network calls, etc.)
 */
class UpdateOperationNoOpRowSkipper extends StandardIterators.BaseStandardIterator<ExecRow> {

    private StandardIterator<ExecRow> rowIterator;
    private FormatableBitSet heapList;
    private int[] colPositionMap;
    private int skippedRows;

    public UpdateOperationNoOpRowSkipper(StandardIterator<ExecRow> rowIterator, FormatableBitSet heapList, int[] colPositionMap) {
        this.rowIterator = rowIterator;
        this.heapList = heapList;
        this.colPositionMap = colPositionMap;
    }

    /* Loop over rows from source it returns null or returns a non no-op row. */
    @Override
    public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws IOException, StandardException {
        skippedRows = 0;
        do {
            ExecRow execRow = rowIterator.next(spliceRuntimeContext);
            if (execRow == null) {
                /* Done reading */
                return null;
            }
            DataValueDescriptor[] sourRowValues = execRow.getRowArray();
            for (int i = heapList.anySetBit(), oldPos = 0; i >= 0; i = heapList.anySetBit(i), oldPos++) {
                DataValueDescriptor oldVal = sourRowValues[oldPos];
                DataValueDescriptor newVal = sourRowValues[colPositionMap[i]];
                if (!newVal.equals(oldVal)) {
                    return execRow;
                }
            }
            skippedRows++;
        } while (true);
    }

    /* The number of rows skipped in the last invocation of next */
    public int getSkippedRows() {
        return skippedRows;
    }

    @Override
    public void open() throws StandardException, IOException {
        rowIterator.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        rowIterator.close();
    }
}