package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.JoinSideExecRow;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardPushBackIterator;
import com.splicemachine.si.impl.PushBackIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
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

    StandardPushBackIterator<JoinSideExecRow> source;
    List<ExecRow> currentRights = new ArrayList<ExecRow>();
    byte[] currentRightHash;
    private int leftRowsSeen;
    private int rightRowsSeen;


    public MergeSortJoinRows(StandardIterator<JoinSideExecRow> source){
        this.source = new StandardPushBackIterator<JoinSideExecRow>(source);
    }

    Pair<byte[],List<ExecRow>> accumulateRights() throws StandardException, IOException {
        JoinSideExecRow row;
        while ((row = source.next(null)) != null){
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

    Pair<ExecRow,Iterator<ExecRow>> nextLeftAndRights() throws StandardException, IOException {
        Pair<byte[],List<ExecRow>> rights = accumulateRights();
        JoinSideExecRow left = source.next(null);
        if (left != null){
            leftRowsSeen++;
            List<ExecRow> rightRows = left.sameHash(rights.getFirst()) ?
                                        rights.getSecond() : (List<ExecRow>)Collections.EMPTY_LIST;
            return new Pair<ExecRow,Iterator<ExecRow>>(left.getRow(), rightRows.iterator());
        }
        return null;
    }

    @Override
    public Pair<ExecRow,Iterator<ExecRow>> next(SpliceRuntimeContext ctx)
            throws StandardException, IOException {
        return nextLeftAndRights();
    }

    @Override
    public void open() throws StandardException, IOException {
        source.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        source.close();
    }

    @Override
    public long getLeftRowsSeen() {
        return leftRowsSeen;
    }

    @Override
    public long getRightRowsSeen() {
        return rightRowsSeen;
    }
}
