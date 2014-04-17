package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.utils.*;
import com.splicemachine.stats.Counter;
import com.splicemachine.stats.MetricFactory;
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

    private final ExecRow mergedRowTemplate;
    private final IJoinRowsIterator<ExecRow> joinRowsSource;
    private final boolean isOuterJoin;
    private final boolean wasRightOuterJoin;
    private final int leftNumCols;
    private final int rightNumCols;
    private final Restriction mergeRestriction;
    private final boolean oneRowRightSide;
    private final boolean antiJoin;
    private final StandardSupplier<ExecRow> emptyRowSupplier;


    public Joiner(IJoinRowsIterator<ExecRow> joinRowsSource,
                  ExecRow mergedRowTemplate,
                  boolean isOuterJoin,
                  boolean wasRightOuterJoin,
                  int leftNumCols,
                  int rightNumCols,
                  boolean oneRowRightSide,
                  boolean antiJoin,
                  StandardSupplier<ExecRow> emptyRowSupplier) {
        this(joinRowsSource, mergedRowTemplate, Restriction.noOpRestriction, isOuterJoin,
                wasRightOuterJoin, leftNumCols, rightNumCols, oneRowRightSide, antiJoin,
                emptyRowSupplier);
    }

    public Joiner(IJoinRowsIterator<ExecRow> joinRowsSource,
									ExecRow mergedRowTemplate,
									Restriction mergeRestriction,
                                    boolean isOuterJoin,
									boolean wasRightOuterJoin,
									int leftNumCols,
									int rightNumCols,
									boolean oneRowRightSide,
									boolean antiJoin,
									StandardSupplier<ExecRow> emptyRowSupplier) {
        this.isOuterJoin = isOuterJoin;
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

    protected boolean shouldMergeEmptyRow(boolean recordsFound) {
        return !recordsFound && (isOuterJoin || antiJoin);
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
            if (!rightSideReturned && shouldMergeEmptyRow(foundRows)) {
                // if we've consumed right side iterator without finding a match & we return empty rows,
                // return with empty right side
                rightSideReturned = true;
                return getMergedRow(currentLeftRow, emptyRowSupplier.get());
            } else {
                /*
                 * We've exhausted the right hand side, and we aren't going to return a match this time.
                 * However, if this is an antiJoin, then we could potentially have returned a match if we
                 * saw the left hand side one too many times. To prevent that, we clear the left side of the
                 * join here.
                 */
                currentLeftRow = null;
            }
        }
        return null;

    }

    public ExecRow nextRow() throws StandardException, IOException {
        Pair<ExecRow,Iterator<ExecRow>> sourcePair;

        ExecRow row = getNextFromBuffer();
        while (row == null
                   && (sourcePair = joinRowsSource.next(null)) != null) {
            addLeftAndRights(sourcePair);
            row = getNextFromBuffer();
        }
        return row;
    }

    public void open() throws StandardException, IOException {
        joinRowsSource.open();
    }

    public void close() throws StandardException, IOException {
        joinRowsSource.close();
    }

    public int getLeftRowsSeen() {
        return joinRowsSource.getLeftRowsSeen();
    }

    public int getRightRowsSeen() {
        return joinRowsSource.getRightRowsSeen();
    }

}
