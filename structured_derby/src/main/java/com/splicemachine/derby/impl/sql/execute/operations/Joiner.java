package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.utils.*;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class Joiner {
    private static Logger LOG = Logger.getLogger(Joiner.class);

    private Iterator<ExecRow> rightSideRowIterator;
    private ExecRow currentLeftRow;
    private boolean rightSideReturned;
    private ExecRow mergedRowTemplate;

    private final IJoinRowsIterator<ExecRow> joinRowsSource;
    private final boolean wasRightOuterJoin;
    private final int leftNumCols;
    private final int rightNumCols;
    private final Restriction mergeRestriction;
    private final boolean oneRowRightSide;
    private final boolean antiJoin;
    private final StandardSupplier<ExecRow> emptyRowSupplier;


    public Joiner(IJoinRowsIterator<ExecRow> joinRowsSource,
                  ExecRow mergedRowTemplate,
                  boolean wasRightOuterJoin,
                  int leftNumCols,
                  int rightNumCols,
                  boolean oneRowRightSide,
                  boolean antiJoin,
                  StandardSupplier<ExecRow> emptyRowSupplier) {
        this(joinRowsSource, mergedRowTemplate, Restriction.noOpRestriction, wasRightOuterJoin,
                leftNumCols, rightNumCols, oneRowRightSide, antiJoin, emptyRowSupplier);
    }

    public Joiner(IJoinRowsIterator<ExecRow> joinRowsSource,
                  ExecRow mergedRowTemplate,
                  Restriction mergeRestriction,
                  boolean wasRightOuterJoin,
                  int leftNumCols,
                  int rightNumCols,
                  boolean oneRowRightSide,
                  boolean antiJoin,
                  StandardSupplier<ExecRow> emptyRowSupplier) {
        this.wasRightOuterJoin = wasRightOuterJoin;
        this.leftNumCols = leftNumCols;
        this.rightNumCols = rightNumCols;
        this.mergedRowTemplate = mergedRowTemplate;
        this.joinRowsSource = joinRowsSource;
        this.mergeRestriction = mergeRestriction;
        this.oneRowRightSide = oneRowRightSide;
        this.antiJoin = antiJoin;
        if (emptyRowSupplier == null)
            emptyRowSupplier = StandardSuppliers.emptySupplier();
        this.emptyRowSupplier = emptyRowSupplier;
    }

    private ExecRow getMergedRow(ExecRow left, ExecRow right) {
        return JoinUtils.getMergedRow(left, right, wasRightOuterJoin, rightNumCols, leftNumCols, mergedRowTemplate);
    }

    private void addLeftAndRights(Pair<ExecRow, Iterator<ExecRow>> leftAndRights) {
        currentLeftRow = leftAndRights.getFirst();
        rightSideRowIterator = leftAndRights.getSecond();
        rightSideReturned = false;
    }

    private ExecRow getNextFromBuffer() throws StandardException {
        if (currentLeftRow != null && rightSideRowIterator != null) {
            boolean foundRows = false;
            while (rightSideRowIterator.hasNext()) {
                ExecRow candidate = getMergedRow(currentLeftRow, rightSideRowIterator.next());
                if (!mergeRestriction.apply(candidate)) {
                    // if doesn't match restriction, discard row
                    continue;
                }
                if (antiJoin) {
                    // if antijoin, discard row but remember that we found a match
                    foundRows = true;
                    continue;
                }
                if (oneRowRightSide) {
                    // before we return a row: if we need to return only one, ignore the rest
                    rightSideRowIterator = null;
                }
                rightSideReturned = true;
                return candidate;
            }
            if (!rightSideReturned && shouldMergeEmptyRow(!foundRows)) {
                // if we've consumed right side iterator without finding a match & we return empty rows,
                // return with empty right side
                rightSideReturned = true;
                return getMergedRow(currentLeftRow, emptyRowSupplier.get());
            }
        }
        return null;

    }

    public ExecRow nextRow() throws StandardException, IOException {
        ExecRow row = getNextFromBuffer();
        while (row == null && joinRowsSource.hasNext()) {
            addLeftAndRights(joinRowsSource.next());
            row = getNextFromBuffer();
        }
        return row;

    }

    public int getLeftRowsSeen() {
        return joinRowsSource.getLeftRowsSeen();
    }

    public int getRightRowsSeen() {
        return joinRowsSource.getRightRowsSeen();
    }

    protected boolean shouldMergeEmptyRow(boolean noRecordsFound) {
        return noRecordsFound && antiJoin;
    }
}
