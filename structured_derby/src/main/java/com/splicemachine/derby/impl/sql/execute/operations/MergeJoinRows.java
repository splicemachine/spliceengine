package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.IOStandardIterator;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardPushBackIterator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * IJoinRowsIterator for MergeJoin.
 *
 * MergeJoin expects both its left & right resultsets to be sorted on a join key
 * (an equijoin key, specifically). This iterator performs the merge based on equality
 * of the join key, and produces pairs of a left row and any matching right rows.
 *
 * The iterator is not thread safe, uses mutable lists to store rows, & must be consumed iteratively.
 *
 * @author P Trolard
 *         Date: 18/11/2013
 */
public class MergeJoinRows implements IJoinRowsIterator<ExecRow> {
    private static final Logger LOG = Logger.getLogger(MergeJoinRows.class);

    final StandardIterator<ExecRow> leftRS;
    final StandardPushBackIterator<ExecRow> rightRS;
    final int[] joinKeys;
    List<ExecRow> currentRights = new LinkedList<ExecRow>();
    Pair<ExecRow, Iterator<ExecRow>> pair;
    private int leftRowsSeen;
    private int rightRowsSeen;

    /**
     * MergeJoinRows constructor. Note that keys for left & right sides
     * are the join keys on which each side is sorted (not all of the
     * join keys).
     *
     * @param leftRS        Iterator for left side rows
     * @param rightRS       Iterator for right side rows
     * @param leftKeys      Join key(s) on which left side is sorted
     * @param rightKeys     Join Key(s) on which right side is sorted
     */
    public MergeJoinRows(StandardIterator<ExecRow> leftRS,
                         IOStandardIterator<ExecRow> rightRS,
                         int[] leftKeys, int[] rightKeys) {
        this.leftRS = leftRS;
        this.rightRS = new StandardPushBackIterator<ExecRow>(rightRS);

        assert(leftKeys.length == rightKeys.length);
        joinKeys = new int[leftKeys.length * 2];
        for (int i = 0, s = leftKeys.length; i < s; i++){
            // add keys indices for left & right like [L1, R1, L2, R2, ...],
            // incremented to work with 1-based ExecRow.getColumn()
            joinKeys[i * 2] = leftKeys[i] + 1;
            joinKeys[i * 2 + 1] = rightKeys[i] + 1;
        }
        pair = new Pair<ExecRow, Iterator<ExecRow>>();
    }

    private int compare(ExecRow left, ExecRow right) throws StandardException {
        for (int i = 0, s = joinKeys.length; i < s; i = i + 2) {
            int result = left.getColumn(joinKeys[i])
                             .compare(right.getColumn(joinKeys[i + 1]));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    Iterator<ExecRow> rightsForLeft(ExecRow left)
            throws StandardException, IOException {
        // Check to see if we've already collected the right rows
        // that match this left
        if (currentRights.size() > 0
                && compare(left, currentRights.get(0)) == 0){
            return currentRights.iterator();
        }
        // If not, look for the ones that do
        currentRights = new LinkedList<ExecRow>();
        ExecRow right;
        while ((right = rightRS.next(null)) != null){
            rightRowsSeen++;
            int comparison = compare(left, right);
            // if matches left, add to buffer
            if (comparison == 0) {
                currentRights.add(right.getClone());
            // if is greater than left, push back & stop
            } else if (comparison < 0) {
                rightRowsSeen--;
                rightRS.pushBack(right);
                break;
            }
            // if is less than left, read next right
        }
        return currentRights.iterator();
    }

    @Override
    public Pair<ExecRow, Iterator<ExecRow>> next(SpliceRuntimeContext ctx)
            throws StandardException, IOException {
        ExecRow left = leftRS.next(ctx);
        if (left != null){
            leftRowsSeen++;
            pair.setFirst(left);
            pair.setSecond(rightsForLeft(left));
            return pair;
        }
        return null;
    }

    @Override
    public long getLeftRowsSeen() {
        return leftRowsSeen;
    }

    @Override
    public long getRightRowsSeen() {
        return rightRowsSeen;
    }

		@Override
    public void open() throws StandardException, IOException {
        leftRS.open();
        rightRS.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        leftRS.close();
        rightRS.close();
        pair = null;
    }
}
