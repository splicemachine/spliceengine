package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.utils.JoinSideExecRow;
import com.splicemachine.si.impl.PushBackIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Pair;

/**
 * IJoinRowsIterator for MergeSortJoin.
 *
 * MergeSortJoin writes its source rows to TEMP keyed by a hash: a sortable
 * byte[] made from the join columns plus an indication of whether the row is from
 * the right or left side. For rows with a given hash, MergeSortJoin writes all
 * right side rows, then all left side rows. Thus, when reading records, we see
 *  {@code right,right,right...,right,left,left,left,...}
 *
 * This class takes an iterator of JoinSideExecRows (which know about the hash and
 * left or right side) and, for every left row, produces a pair of the left row
 * and any right rows with a matching hash.
 *
 * This iterator is not thread-safe, and uses mutable lists for storing rows. The only
 * safe way to consume it is iteratively (i.e. not realizing the sequence) or by copying
 * elements.
 *
 * @author P Trolard
 *         Date: 13/11/2013
 */
public class MergeSortJoinRows implements IJoinRowsIterator<ExecRow> {

    PushBackIterator<JoinSideExecRow> source;
    List<ExecRow> currentRights = new ArrayList<ExecRow>();
    byte[] currentRightHash;
    Pair<ExecRow,Iterator<ExecRow>> nextBatch;
    private int leftRowsSeen;
    private int rightRowsSeen;


    public MergeSortJoinRows(Iterator<JoinSideExecRow> source){
        this.source = new PushBackIterator<JoinSideExecRow>(source);
    }

    Pair<byte[],List<ExecRow>> accumulateRights(){
        while (source.hasNext()){
            JoinSideExecRow row = source.next();
            if (row.isRightSide()){
                rightRowsSeen++;
                if (row.sameHash(currentRightHash)){
                    // must use getRow().getClone() b/c underlying source mutates
                    currentRights.add(row.getRow().getClone());
                } else {
                    currentRights = new ArrayList<ExecRow>();
                    currentRights.add(row.getRow().getClone());
                    currentRightHash = row.getHash();
                }
            } else {
                source.pushBack(row);
                break;
            }
        }
        return new Pair<byte[],List<ExecRow>>(currentRightHash, currentRights);
    }

    Pair<ExecRow,Iterator<ExecRow>> nextLeftAndRights(){
        Pair<byte[],List<ExecRow>> rights = accumulateRights();
        if (source.hasNext()){
            leftRowsSeen++;
            JoinSideExecRow left = source.next();
            List<ExecRow> rightRows = left.sameHash(rights.getFirst()) ?
                                        rights.getSecond() : (List<ExecRow>)Collections.EMPTY_LIST;
            return new Pair<ExecRow,Iterator<ExecRow>>(left.getRow(), rightRows.iterator());
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        if (nextBatch == null){
            nextBatch = nextLeftAndRights();
        }
        return nextBatch != null;
    }

    @Override
    public Pair<ExecRow,Iterator<ExecRow>> next() {
        Pair<ExecRow,Iterator<ExecRow>> value = nextBatch;
        nextBatch = null;
        return value;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Pair<ExecRow, Iterator<ExecRow>>> iterator() {
        return this;
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
