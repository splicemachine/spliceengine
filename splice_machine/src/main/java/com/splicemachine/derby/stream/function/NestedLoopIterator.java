package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.NestedLoopJoinOperation;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Iterator;

public class NestedLoopIterator implements Iterator<LocatedRow>, Iterable<LocatedRow> {
    private static Logger LOG = Logger.getLogger(NestedLoopIterator.class);
    protected LocatedRow leftRow;
    protected DataSet rightDataSet;
    protected NestedLoopJoinOperation operation;
    private boolean populated;
    protected Iterator<LocatedRow> rightSideIterator;
    private boolean returnedRight=false;
    protected LocatedRow populatedRow;
    protected int totalColumns;


    public NestedLoopIterator(LocatedRow leftRow, NestedLoopJoinOperation operation,
                              Iterator<LocatedRow> rightSideIterator,
                       SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "NestedLoopIterator instantiated with leftRow %s", leftRow);
        this.leftRow = leftRow;
        this.operation = operation;
        this.totalColumns = operation.getLeftNumCols()+operation.getRightNumCols();
        this.rightSideIterator = rightSideIterator;
    }

    @Override
    public boolean hasNext() {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, ">>> NestedLoopIterator hasNext()");
        if(populated) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, ">>> NestedLoopIterator populated=true");
            return true;
        }

        try {
            populated = false;
            LocatedRow rightSideRow = null;

            // Not Exists Logic:
            // This may not exhaust the iterator (i.e. keeping it open)
            // Will Require Calling Function to explicitly close
            if (operation.notExistsRightSide) {
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG, ">>> NestedLoopIterator notExistsRightSide");
                while (rightSideIterator.hasNext() && (rightSideRow = rightSideIterator.next()) != null) {
                    ExecRow mergedRow = operation.getActivation().getExecutionFactory().getValueRow(totalColumns);
                    mergedRow = JoinUtils.getMergedRow(leftRow.getRow(),rightSideRow.getRow(),operation.wasRightOuterJoin,operation.getLeftNumCols(),operation.getRightNumCols(),mergedRow);
                    if (!operation.getRestriction().apply(mergedRow)) {// Row is filtered, keep iterating
                        if (LOG.isDebugEnabled())
                            SpliceLogUtils.debug(LOG, ">>> NestedLoopIterator notExists restriction removed left=%s, right=%s",leftRow.getRow(),rightSideRow.getRow());
                        continue;
                    }
                    else { // Found a row
                        if (LOG.isDebugEnabled())
                            SpliceLogUtils.debug(LOG, ">>> NestedLoopIterator notExistsFound, removed left=%s, right=%s",leftRow.getRow(),rightSideRow.getRow());

                        populated = false;
                        return false;
                    }
                }
                ExecRow mergedRow = operation.getActivation().getExecutionFactory().getValueRow(totalColumns);
                populatedRow = new LocatedRow(JoinUtils.getMergedRow(leftRow.getRow(),operation.getEmptyRow(),operation.wasRightOuterJoin,operation.getLeftNumCols(),operation.getRightNumCols(),mergedRow));
                populated=true;
                return true;
            }

            while (rightSideIterator.hasNext() && (rightSideRow = rightSideIterator.next()) != null) {
                if (operation.oneRowRightSide && returnedRight) { // Already returned right side ?InnerJoin?
                    if (LOG.isDebugEnabled())
                        SpliceLogUtils.debug(LOG, ">>> NestedLoopIterator one row right side and returned already, removed left=%s, right=%s",leftRow.getRow(),rightSideRow.getRow());
                    returnedRight = false;
                    populated=false;
                    return false;
                }
                ExecRow mergedRow = operation.getActivation().getExecutionFactory().getValueRow(totalColumns);
                populatedRow = new LocatedRow(JoinUtils.getMergedRow(leftRow.getRow(),rightSideRow.getRow(),operation.wasRightOuterJoin,operation.getLeftNumCols(),operation.getRightNumCols(),mergedRow));
                if (!operation.getRestriction().apply(populatedRow.getRow())) {
                    if (LOG.isDebugEnabled())
                        SpliceLogUtils.debug(LOG, ">>> NestedLoopIterator restriction removed left=%s, right=%s",leftRow.getRow(),rightSideRow.getRow());
                    continue;
                }
                else {
                    if (LOG.isDebugEnabled())
                        SpliceLogUtils.debug(LOG, ">>> NestedLoopIterator added left=%s, right=%s",leftRow.getRow(),rightSideRow.getRow());
                    populated = true;
                    return true;
                }
            }
            populated = false;
            return false;
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }

    }

    @Override
    public LocatedRow next() {
        SpliceLogUtils.trace(LOG, "next row=%s",populatedRow);
        populated=false;
        return populatedRow;
    }

    @Override
    public void remove() {
        SpliceLogUtils.trace(LOG, "remove");
    }
    @Override
    public Iterator<LocatedRow> iterator() {
        return this;
    }
}
