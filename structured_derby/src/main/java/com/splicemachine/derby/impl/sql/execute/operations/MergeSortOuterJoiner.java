package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.util.Collection;

/**
 * @author Scott Fines
 *         Created on: 10/29/13
 */
public class MergeSortOuterJoiner extends MergeSortJoiner {
    private ExecRow emptyRow;

    private final StandardSupplier<ExecRow> emptyRowSupplier;

    public MergeSortOuterJoiner(ExecRow mergedRowTemplate,
                                MergeScanner scanner,
                                boolean wasRightOuterJoin,
                                int leftNumCols,
                                int rightNumCols,
                                boolean oneRowRightSide,
                                boolean notExistsRightSide,
                                StandardSupplier<ExecRow> emptyRowSupplier) {
        super(mergedRowTemplate, scanner, wasRightOuterJoin, leftNumCols, rightNumCols, oneRowRightSide, notExistsRightSide);
        this.emptyRowSupplier = emptyRowSupplier;
    }

    public MergeSortOuterJoiner(ExecRow mergedRowTemplate,
                                MergeScanner scanner,
                                Restriction mergeRestriction,
                                boolean wasRightOuterJoin,
                                int leftNumCols,
                                int rightNumCols,
                                boolean oneRowRightSide,
                                boolean notExistsRightSide,
                                StandardSupplier<ExecRow> emptyRowSupplier) {
        super(mergedRowTemplate, scanner, mergeRestriction, wasRightOuterJoin, leftNumCols, rightNumCols, oneRowRightSide, notExistsRightSide);
        this.emptyRowSupplier = emptyRowSupplier;
    }

    @Override
    protected boolean shouldMergeEmptyRow(boolean noRecordsFound) {
        return noRecordsFound;
    }

    @Override
    protected ExecRow getEmptyRow() throws StandardException {
        if(emptyRow==null){
            emptyRow = emptyRowSupplier.get();
        }
        return emptyRow;
    }

}
