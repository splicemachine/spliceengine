package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardPushBackIterator;
import com.splicemachine.si.impl.PushBackIterator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

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
    final Comparator<ExecRow> comparator;
    final List<Pair<Integer,Integer>> joinKeys;
    List<ExecRow> currentRights = new ArrayList<ExecRow>();

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
                         StandardIterator<ExecRow> rightRS,
                         int[] leftKeys, int[] rightKeys) {
        this.leftRS = leftRS;
        this.rightRS = new StandardPushBackIterator<ExecRow>(rightRS);

        assert(leftKeys.length == rightKeys.length);
        joinKeys = Lists.newArrayListWithCapacity(leftKeys.length);
        for (int i = 0, s = leftKeys.length; i < s; i++){
            // add keys for left & right, incremented to work with 1-based ExecRow.getColumn()
            joinKeys.add(Pair.newPair(leftKeys[i] + 1, rightKeys[i] + 1));
        }
        comparator = new Comparator<ExecRow>() {
            @Override
            public int compare(ExecRow left, ExecRow right) {
                try {

                    for (Pair<Integer,Integer> keys: joinKeys){
                        int result = left.getColumn(keys.getFirst())
                                        .compare(right.getColumn(keys.getSecond()));
                        if (result != 0){
                            return result;
                        }
                    }
                    return 0;

                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    Iterator<ExecRow> rightsForLeft(ExecRow left)
            throws StandardException, IOException {
        // Check to see if we've already collected the right rows
        // that match this left
        if (currentRights.size() > 0
                && comparator.compare(left, currentRights.get(0)) == 0){
            return currentRights.iterator();
        }
        // If not, look for the ones that do
        currentRights = new ArrayList<ExecRow>();
        ExecRow right;
        while ((right = rightRS.next(null)) != null){
            rightRowsSeen++;
            int comparison = comparator.compare(left, right);
            // if matches left, add to buffer
            if (comparison == 0) {
                currentRights.add(right.getClone());
            // if is greater than left, push back & stop
            } else if (comparison == -1) {
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
            return Pair.newPair(left, rightsForLeft(left));
        }
        return null;
    }

    @Override
    public int getLeftRowsSeen() {
        return leftRowsSeen;
    }

    @Override
    public int getRightRowsSeen() {
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
    }
}
