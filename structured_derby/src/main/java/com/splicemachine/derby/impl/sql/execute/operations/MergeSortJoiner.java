package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.utils.JoinSideExecRow;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.StandardSuppliers;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

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
    private Collection<ExecRow> rightSideRows;
    private Iterator<ExecRow> rightSideRowIterator;
    private ExecRow currentLeftRow;
    private ExecRow mergedRowTemplate;
    private byte[] currentHash;

    private final StandardIterator<JoinSideExecRow> scanner;
    private final boolean wasRightOuterJoin;
    private final int leftNumCols;
    private final int rightNumCols;
    private final Restriction mergeRestriction;
    private final boolean oneRowRightSide;
    private final boolean antiJoin;

    private byte[] currentRowKey;
    private boolean rightSideReturned;
    private final StandardSupplier<ExecRow> emptyRowSupplier;

		private int leftRowsSeen = 0;
		private int rightRowsSeen=0;

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
        this.mergeRestriction = mergeRestriction;
        this.oneRowRightSide = oneRowRightSide;
        this.antiJoin = antiJoin;
        if(emptyRowSupplier==null)
            emptyRowSupplier = StandardSuppliers.emptySupplier();
        this.emptyRowSupplier = emptyRowSupplier;
    }

    public ExecRow nextRow() throws StandardException,IOException {
        /*
         * The data for MergeSort is sorted as [Right Rows with join hash], [Left rows with join hash],
         * [Right rows with next join hash], etc.
         *
         * So the basic algorithm is:
         *
         * 1. Read all right rows which have the same hash key into a buffer
         * 2. Read each left row which has the same hash as the right rows
         *  A. For each right row, join to the left row and emit
         */
        ExecRow mergedRow= getNextMergedRow();
        if(mergedRow!=null)
            return mergedRow;
        /*
         * Get the next row from the scanner, and act accordingly.
         *
         * There are  possibilities:
         *
         * 1. the next row is a Left Row
         *  A. The hash matches what is currently buffered in rightRows
         *  B. The hash does not match
         * 2. The next row is a Right Row.
         */
        do{
            JoinSideExecRow nextRowToMerge = scanner.next();
            if(nextRowToMerge==null){
                //we are out of rows in the scanner, just return null;
                break;
            }
            currentRowKey = nextRowToMerge.getRowKey();
						if(nextRowToMerge.getJoinSide()== JoinUtils.JoinSide.LEFT){
								leftRowsSeen++;
								mergedRow = mergeLeftRow(nextRowToMerge);
						}else{
								rightRowsSeen++;
                if(nextRowToMerge.sameHash(currentHash))
                    rightSideRows.add(nextRowToMerge.getRow().getClone());
                else{
                /*
                 * We have a new right row, but it doesn't have the same hash
                 * as the old right rows. Reset the hash and the iterator
                 * and try again
                 */
                    if(rightSideRows==null)
                        rightSideRows = Lists.newArrayList();
                    rightSideRows.clear();
                    rightSideRows.add(nextRowToMerge.getRow().getClone());
                    currentHash = nextRowToMerge.getHash();
                    currentLeftRow = null;
                }
                mergedRow=null; //force the loop to continue
            }
        }while(mergedRow==null);

        return mergedRow;
    }

		public int getLeftRowsSeen(){
				return leftRowsSeen;
		}

		public int getRightRowsSeen(){
				return rightRowsSeen;
		}

    private ExecRow mergeLeftRow(JoinSideExecRow nextLeftRow) throws StandardException {
        /*
         * We got a new left row.
         *
         * If the hash for the new left is the same as the hash for the
         * right side, then we just reset the right side iterator, then
         * iterate over it.
         *
         * If the hash doesn't match, then this is left row which has no right
         * rows to join to. Empty rightRows, then skip over these records
         *
         */
        currentLeftRow = nextLeftRow.getRow();
        if(nextLeftRow.sameHash(currentHash)){
            /*
             * We have the same hash as the current right side buffer. Reset the
             * iterator, and merge
             */
            rightSideRowIterator = rightSideRows.iterator();
            rightSideReturned=false;
            return getNextMergedRow();
        }else{
            /*
             * We have a different hash. This means that we have no right rows
             * for this left row hash. In the case of Inner Joins, we'll skip
             * this row, but outer rows, we will return null for all the right
             * columns.
             *
             * Either way, clear the right side rows and set the hash, then
             * loop
             */
            if(rightSideRows==null)
                rightSideRows = Lists.newArrayList();
            rightSideRows.clear();
            rightSideRowIterator = rightSideRows.iterator();
            currentHash = nextLeftRow.getHash();
            rightSideReturned=false;
            return getNextMergedRow();
        }
    }

    private ExecRow getNextMergedRow() throws StandardException {
        /*
         * Merge left and right rows to get the next row to return.
         *
         * It is assumed that the left row is present.
         *
         * The flow of this is as follows:
         *
         * 1. Merge a left and a right row together (if a right row exists).
         * 2. Check predicate. If Predicate fails, throw away this merged row and go to the next right row (return to 1).
         * 3. If require only a single right side for this left, and a row has already been emitted, go to next right row (return to 1).
         * 4. If an antiJoin, then go to next right row (return to 1)
         * 5. If no rows have been returned (e.g. no right rows are available):
         *  A. If outerjoin, merge with empty row
         *  B. if antiJoin, merge with empty row
         *  C. Otherwise, skip this left row.
         */
        boolean noRecordsFound = currentLeftRow!=null;
        if(rightSideRowIterator!=null){
            while(rightSideRowIterator.hasNext()){
                ExecRow nextRightRow = rightSideRowIterator.next();
                ExecRow mergedRow = JoinUtils.getMergedRow(currentLeftRow,nextRightRow,wasRightOuterJoin,rightNumCols,leftNumCols,mergedRowTemplate);
                if(!mergeRestriction.apply(mergedRow))
                    continue;
                if(oneRowRightSide && rightSideReturned)
                    continue; //skip rows if oneRowRightSide is required
                noRecordsFound=false;
                if(antiJoin)
                    continue; //antijoin miss
                rightSideReturned=true;
                return mergedRow;
            }
            //restriction meant everything was stripped out. Just move on
        }
        if(!rightSideReturned&&shouldMergeEmptyRow(noRecordsFound)){
            rightSideReturned=true;
            ExecRow rightRow = emptyRowSupplier.get();
            return JoinUtils.getMergedRow(currentLeftRow,rightRow,wasRightOuterJoin,rightNumCols,leftNumCols,mergedRowTemplate);
        }
        return null;
    }

    protected boolean shouldMergeEmptyRow(boolean noRecordsFound) {
        return noRecordsFound && antiJoin;
    }

    public byte[] lastRowLocation() {
        return currentRowKey;
    }

    public void close() throws IOException, StandardException {
        scanner.close();
    }
}

