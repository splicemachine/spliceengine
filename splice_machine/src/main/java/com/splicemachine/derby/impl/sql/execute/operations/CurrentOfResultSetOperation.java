/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.*;
import com.splicemachine.db.iapi.sql.execute.CursorActivation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.control.MaterializedControlDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Takes a cursor name and returns the current row
 * of the cursor; for use in generating the source
 * row and row location for positioned update/delete operations.
 * <p>
 * This result set returns only one row.
 *
 */
public class CurrentOfResultSetOperation extends SpliceBaseOperation {

    private CursorResultSet cursor;
    private CursorResultSet target;
    private ExecRow         sparseRow;

    protected static final String NAME = CurrentOfResultSetOperation.class.getSimpleName().replaceAll("Operation", "");

    // set in constructor and not altered during
    // life of object.
    private String cursorName;

    public CurrentOfResultSetOperation() {}

    //
    // class interface
    //
    public CurrentOfResultSetOperation(String cursorName, Activation activation,
                                       int resultSetNumber) throws StandardException {
        super(activation, resultSetNumber, 0.0d, 0.0d);
        if (SanityManager.DEBUG)
            SanityManager.ASSERT( cursorName!=null, "current of scan must get cursor name");
        this.cursorName = cursorName;
    }

    /**
     * Return a sparse heap row, based on a compact index row.
     *
     * @param row  compact referenced index row
     * @param indexCols base column positions of index keys, signed with asc/desc info
     *
     * @return   a sparse heap row with referenced columns
     */
    private ExecRow getSparseRow(ExecRow row, int[] indexCols) throws StandardException
    {
        int colPos;
        if (sparseRow == null)
        {
            int numCols = 1;
            for (int i = 0; i < indexCols.length; i++)
            {
                colPos = (indexCols[i] > 0) ? indexCols[i] : -indexCols[i];
                if (colPos > numCols)
                    numCols = colPos;
            }
            sparseRow = new ValueRow(numCols);
        }
        for (int i = 1; i <= indexCols.length; i++)
        {
            colPos = (indexCols[i-1] > 0) ? indexCols[i-1] : -indexCols[i-1];
            sparseRow.setColumn(colPos, row.getColumn(i));
        }

        return sparseRow;
    }

    /**
     * Return the total amount of time spent in this ResultSet
     *
     * @param type CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
     *    ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
     *
     * @return long  The total amount of time spent (in milliseconds).
     */
    public long getTimeSpent(int type)
    {
        /* RESOLVE - RunTimeStats not implemented yet */
        return 0;
    }

    //
    // class implementation
    //
    /**
        Because the positioned operation only gets one location
        per execution, and the cursor could be completely different
        for each execution (closed and reopened, perhaps), we
        determine where caching the cursor could be applied.
        <p>
        When cached, we check if the cursor was closed'd,
        and if so, throw it out and
        see if there's one in the cache with our name.

     */
    private void getCursor() throws StandardException {

        // need to look again if cursor was closed
        if (cursor != null) {
            if (cursor.isClosed())
            {
                cursor = null;
                target = null;
            }
        }

        if (cursor == null) {

            LanguageConnectionContext lcc = getActivation().getLanguageConnectionContext();

            CursorActivation cursorActivation = lcc.lookupCursorActivation(cursorName);

            if (cursorActivation != null)
            {

                cursor = cursorActivation.getCursorResultSet();
                target = cursorActivation.getTargetResultSet();
                /* beetle 3865: updateable cursor using index. 2 way communication between
                 * update activation and cursor activation. Cursor passes index scan to
                 * update and update passes heap conglom controller to cursor.
                 */
                activation.setForUpdateIndexScan(cursorActivation.getForUpdateIndexScan());
                if (cursorActivation.getHeapConglomerateController() != null)
                    cursorActivation.getHeapConglomerateController().close();
                cursorActivation.setHeapConglomerateController(activation.getHeapConglomerateController());
            }
        }

        if (cursor == null || cursor.isClosed()) {
            throw StandardException.newException(SQLState.LANG_CURSOR_NOT_FOUND, cursorName);
        }
    }

    /**
     * @see NoPutResultSet#updateRow
     */
    public void updateRow(ExecRow row, RowChanger rowChanger)
            throws StandardException {
        ((NoPutResultSet)cursor).updateRow(row, rowChanger);
    }

    /**
     * @see NoPutResultSet#markRowAsDeleted
     */
    public void markRowAsDeleted() throws StandardException {
        ((NoPutResultSet)cursor).markRowAsDeleted();
    }

    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException(String.format("Operation %s is not open", NAME));

        getCursor();

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(! cursor.isClosed(), "cursor closed");

        ExecRow cursorRow = cursor.getCurrentRow();

        // requalify the current row
        if (cursorRow == null) {
            throw StandardException.newException(SQLState.NO_CURRENT_ROW);
        }
        // we know it will be requested, may as well get it now.
        setCurrentRowLocation(cursor.getRowLocation());

        // get the row from the base table, which is the real result
        // row for the CurrentOfResultSet
        ExecRow baseRow = cursor.getCurrentBaseRow();
        baseRow.setKey(cursor.getCurrentRow().getKey());
        setCurrentRow(baseRow);

        // if the source result set is a ScrollInsensitiveResultSet, and
        // the current row has been deleted (while the cursor was
        // opened), the cursor result set (scroll insensitive) will
        // return the cached row, while the target result set will
        // return null (row has been deleted under owr feet).
        if (getCurrentRowLocation() == null || getCurrentRow() == null) {
            activation.addWarning(StandardException.newWarning(SQLState.CURSOR_OPERATION_CONFLICT));
            return null;
        }

        /* beetle 3865: updateable cursor using index.  If underlying is a covering
         * index, target is a TableScanRS (instead of a IndexRow2BaseRowRS) for the
         * index scan.  But the problem is it returns a compact row in index key order.
         * However the ProjectRestrictRS above us that sets up the old and new column
         * values expects us to return a sparse row in heap order.  We have to do the
         * wiring here, since we don't have IndexRow2BaseRowRS to do this work.  This
         * problem was not exposed before, because we never used index scan for updateable
         * cursors.
         */
                /*
                if (target instanceof TableScanResultSet)
                {
                    TableScanResultSet scan = (TableScanResultSet) target;
                    if (scan.indexCols != null && currentRow != null)
                        currentRow = getSparseRow(currentRow, scan.indexCols);
                }
                 */

        // REMIND: verify the row is still there
        // at present we get an ugly exception from the store,
        // Hopefully someday we can just do this:
        //
        // if (!rowLocation.rowExists())
        //     throw StandardException.newException(SQLState.LANG_NO_CURRENT_ROW, cursorName);

        List<ExecRow> rows = new ArrayList<>();
        rows.add(getCurrentRow());
        return new MaterializedControlDataSet<>( rows );
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return null;
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return false;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "CurrentOfResultSet";
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.emptyList();
    }
}
