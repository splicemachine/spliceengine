package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author P Trolard
 *         Date: 23/03/2014
 */
public class BroadCastJoinRows implements IJoinRowsIterator<ExecRow> {

    private static final Logger LOG = Logger.getLogger(BroadCastJoinRows.class);

    private final Iterator<ExecRow> leftRows;
    private final Function<ExecRow,List<ExecRow>> rightSideLookup;

    private Pair<ExecRow,Iterator<ExecRow>> nextBatch;
    private int leftRowsSeen;
    private int rightRowsSeen;

    public BroadCastJoinRows(Iterator<ExecRow> leftRows,
                             Function<ExecRow,List<ExecRow>> rightSideLookup) {
        this.leftRows = leftRows;
        this.rightSideLookup = rightSideLookup;
    }


    private Pair<ExecRow,Iterator<ExecRow>> getNextBatch() {
        if (leftRows.hasNext()){
            leftRowsSeen++;
            ExecRow left = leftRows.next();
            List<ExecRow> rights = rightSideLookup.apply(left);
            if (rights == null){
                rights = Collections.EMPTY_LIST;
            }
            // TODO don't count right rows already seen
            rightRowsSeen += rights.size();
            //LOG.error(String.format("BCast join rows batch: L %s R %s",
//                                       left, rights));
            return Pair.newPair(left, rights.iterator());
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        if (nextBatch == null){
            nextBatch = getNextBatch();
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
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Pair<ExecRow, Iterator<ExecRow>>> iterator() {
        return this;
    }

    @Override
    public int getLeftRowsSeen() {
        return 0;
    }

    @Override
    public int getRightRowsSeen() {
        return 0;
    }
}
