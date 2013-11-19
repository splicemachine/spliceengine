package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.si.impl.PushBackIterator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * IJoinRowsIterator for MergeJoin.
 *
 * MergeJoin expects both its left & right resultsets to be sorted on a join key
 * (an equijoin key, specifically). This iterator performs the merge based on equality
 * of the join key, and produces pairs of a left row and any
 * matching right rows.
 *
 * The iterator is not thread safe, uses mutable lists to store rows, & must be consumed iteratively.
 *
 * @author P Trolard
 *         Date: 18/11/2013
 */
public class MergeJoinRows implements IJoinRowsIterator<ExecRow> {
    private static final Logger LOG = Logger.getLogger(MergeJoinRows.class);

    Iterator<ExecRow> leftRS;
    PushBackIterator<ExecRow> rightRS;
    Comparator<ExecRow> comparator;
    final List<Pair<Integer,Integer>> joinKeys;
    List<ExecRow> currentRights = new ArrayList<ExecRow>();

    private Pair<ExecRow,Iterator<ExecRow>> nextBatch;
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
    public MergeJoinRows(Iterator<ExecRow> leftRS, Iterator<ExecRow> rightRS,
                         int[] leftKeys, int[] rightKeys) {
        this.leftRS = leftRS;
        this.rightRS = new PushBackIterator<ExecRow>(rightRS);

        assert(leftKeys.length == rightKeys.length);
        joinKeys = new ArrayList<Pair<Integer,Integer>>(leftKeys.length);
        for (int i = 0, s = leftKeys.length; i < s; i++){
            joinKeys.add(Pair.newPair(leftKeys[i], rightKeys[i]));
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

    Iterator<ExecRow> rightsForLeft(ExecRow left){
        // Check to see if we've already collected the right rows
        // that match this left
        if (currentRights.size() > 0
                && comparator.compare(left, currentRights.get(0)) == 0){
            return currentRights.iterator();
        }
        // If not, look for the ones that do
        currentRights.clear();
        while (rightRS.hasNext()){
            rightRowsSeen++;
            ExecRow right = rightRS.next();
            int compared = comparator.compare(left, right);
            if (compared == 0) {
                currentRights.add(right);
            } else if (compared == -1) {
                rightRS.pushBack(right);
                break;
            }
        }
        return currentRights.iterator();
    }

    Pair<ExecRow,Iterator<ExecRow>> getNextLeftAndRights(){
        if (leftRS.hasNext()){
            leftRowsSeen++;
            ExecRow left = leftRS.next();
            return Pair.newPair(left, rightsForLeft(left));
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        if (nextBatch == null){
            nextBatch = getNextLeftAndRights();
        }
        return nextBatch != null;
    }

    @Override
    public Pair<ExecRow, Iterator<ExecRow>> next() {
        Pair<ExecRow,Iterator<ExecRow>> value = nextBatch;
        nextBatch = null;
        return value;
    }

    @Override
    public Iterator<Pair<ExecRow, Iterator<ExecRow>>> iterator() {
        return this;
    }

     @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLeftRowsSeen() {
        return leftRowsSeen;
    }

    @Override
    public int getRightRowsSeen() {
        return rightRowsSeen;
    }
}
