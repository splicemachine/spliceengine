package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.impl.store.access.hbase.ByteArraySlice;
import com.splicemachine.derby.utils.JoinSideExecRow;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.StandardSuppliers;
import com.splicemachine.utils.SpliceLogUtils;

import java.io.IOException;
import java.util.Iterator;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import static com.splicemachine.derby.utils.StandardIterators.StandardIteratorIterator;

/**
 * Responsible for Joining Rows which follow the MergeSortJoin pattern.
 *
 * The MergeSortJoin storage pattern is to write all rows according to a "hash"--that is,
 * a sortable byte[] based on the join keys. After the hash comes an ordinal which
 * refers to which "side" (right or left) of the join the row came from.
 *
 * By convention, MergeSortJoin writes all right side rows, then all left side
 * rows which have the same hash. Thus, when reading records, the joiner will
 * see {@code right,right,right...,right,left,left,left,...}
 *
 * This class is designed for overriding (particularly in the case of Outer joins).
 * The default implementation is for a Left inner join.
 *
 * This class is <em>not</em> thread-safe.
 *
 * @author Scott Fines
 * Created on: 10/29/13
 */
public class MergeSortJoiner {
    private static Logger LOG = Logger.getLogger(MergeSortJoiner.class);

    private Iterator<ExecRow> rightSideRowIterator;
    private ExecRow currentLeftRow;
    private ExecRow mergedRowTemplate;

    private final StandardIterator<JoinSideExecRow> scanner;
    // the bridgeScanner bridges between the StandardIterator and vanilla Iterator interfaces
    private final StandardIteratorIterator<JoinSideExecRow> bridgeScanner;
    private final IJoinRowsIterator<ExecRow> joinRowsSource;
    private final boolean wasRightOuterJoin;
    private final int leftNumCols;
    private final int rightNumCols;
    private final Restriction mergeRestriction;
    private final boolean oneRowRightSide;
    private final boolean antiJoin;

    private ByteArraySlice currentRowKey;
    private boolean rightSideReturned;
    private final StandardSupplier<ExecRow> emptyRowSupplier;
    private boolean isClosed;

    public MergeSortJoiner(ExecRow mergedRowTemplate,
                           StandardIterator<JoinSideExecRow> scanner,
                           boolean wasRightOuterJoin,
                           int leftNumCols,
                           int rightNumCols,
                           boolean oneRowRightSide,
                           boolean antiJoin,
                           StandardSupplier<ExecRow> emptyRowSupplier) {
        this(mergedRowTemplate, scanner, Restriction.noOpRestriction,wasRightOuterJoin,
                leftNumCols, rightNumCols,oneRowRightSide, antiJoin,emptyRowSupplier);
    }

    public MergeSortJoiner(ExecRow mergedRowTemplate,
                           StandardIterator<JoinSideExecRow> scanner,
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
        this.scanner = scanner;
        this.bridgeScanner = StandardIterators.asIter(scanner);
        this.joinRowsSource = new MergeSortJoinRows(bridgeScanner);
        this.mergeRestriction = mergeRestriction;
        this.oneRowRightSide = oneRowRightSide;
        this.antiJoin = antiJoin;
        if(emptyRowSupplier==null)
            emptyRowSupplier = StandardSuppliers.emptySupplier();
        this.emptyRowSupplier = emptyRowSupplier;
    }

    private ExecRow getMergedRow(ExecRow left, ExecRow right){
        SpliceLogUtils.debug(LOG, ">>>     MergeSortJoiner Right: ",(right != null ? right : "NULL RIGHT ROW"));
        return JoinUtils.getMergedRow(left, right, wasRightOuterJoin, rightNumCols, leftNumCols, mergedRowTemplate);
    }

    private void addLeftAndRights(Pair<ExecRow,Iterator<ExecRow>> leftAndRights){
        currentLeftRow = leftAndRights.getFirst();
        SpliceLogUtils.debug(LOG, ">>>     MergeSortJoiner Left: ",(currentLeftRow != null ? currentLeftRow : "NULL LEFT ROW"));

        rightSideRowIterator = leftAndRights.getSecond();
        rightSideReturned = false;
    }

    private ExecRow getNextFromBuffer() throws StandardException {
        if (currentLeftRow != null && rightSideRowIterator != null){
            boolean foundRows = false;
            while (rightSideRowIterator.hasNext()){
                ExecRow candidate = getMergedRow(currentLeftRow, rightSideRowIterator.next());
                if (!mergeRestriction.apply(candidate)){
                    // if doesn't match restriction, discard row
                    SpliceLogUtils.debug(LOG, ">>>       MergeSortJoiner Right Discarded");
                    continue;
                }
                if (antiJoin){
                    // if antijoin, discard row but remember that we found a match
                    SpliceLogUtils.debug(LOG, ">>>        MergeSortJoiner Antijoin ");
                    foundRows = true;
                    continue;
                }
                if (oneRowRightSide){
                    // before we return a row: if we need to return only one, ignore the rest
                    SpliceLogUtils.debug(LOG, ">>>       MergeSortJoiner Nulling RightSideRowIterator ");
                    rightSideRowIterator = null;
                }
                rightSideReturned = true;
                return candidate;
            }
            if (!rightSideReturned && shouldMergeEmptyRow(!foundRows)){
                // if we've consumed right side iterator without finding a match & we return empty rows,
                // return with empty right side
                rightSideReturned = true;
                SpliceLogUtils.debug(LOG, ">>>       MergeSortJoiner Consumed all rights. Returning empty right side.");
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
        if (LOG.isDebugEnabled())
        	SpliceLogUtils.debug(LOG, ">>>     MergeSortJoiner Emit: ", (row != null ? row : "NULL TOP ROW"));
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

    public ByteArraySlice lastRowLocation() {
        return currentRowKey;
    }

    public void close() throws IOException, StandardException {
        SpliceLogUtils.debug(LOG, ">>>     MergeSortJoiner closing");
        isClosed = true;
        scanner.close();
        if (bridgeScanner.hasException()){
            bridgeScanner.throwExceptions();
        }
    }

    public void open() {
        this.isClosed = false;
    }

    public boolean isClosed() {
        return isClosed;
    }
}

