package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.metrics.Counter;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author P Trolard
 *         Date: 23/03/2014
 */
public class BroadCastJoinRows implements IJoinRowsIterator<ExecRow> {
    private final StandardIterator<ExecRow> leftRows;
    private final Function<ExecRow,Iterator<ExecRow>> rightSideLookup;
    private Pair<ExecRow, Iterator<ExecRow>> pair;
    private int leftRowsSeen;
    private Counter rightRowsSeen;

    public BroadCastJoinRows(StandardIterator<ExecRow> leftRows,
                             Function<ExecRow,Iterator<ExecRow>> rightSideLookup) {
        this.leftRows = leftRows;
        this.rightSideLookup = rightSideLookup;
        this.pair =new Pair<>();
    }

    @Override
    public Pair<ExecRow, Iterator<ExecRow>> next(SpliceRuntimeContext ctx) throws StandardException, IOException {
        if(rightRowsSeen==null)
            rightRowsSeen = ctx.newCounter();
        ExecRow left = leftRows.next(ctx);
        if (left != null){
            leftRowsSeen++;
            Iterator<ExecRow> rights = rightSideLookup.apply(left);
            if (rights == null){
                rights = Collections.emptyIterator();
            }else{
                rights = new CountingIterator(rights,rightRowsSeen);
            }
            // TODO don't count right rows already seen
//            rightRowsSeen += rights.size();
            pair.setFirst(left);
            pair.setSecond(rights);
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
        return rightRowsSeen.getTotal();
    }

    @Override
    public void open() throws StandardException, IOException {
        leftRows.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        leftRows.close();
        pair = null;
    }

    private class CountingIterator implements Iterator<ExecRow>{
        private final Iterator<ExecRow> delegate;
        private final Counter rightRowsSeen;

        public CountingIterator(Iterator<ExecRow> delegate,Counter rightRowsSeen){
            this.delegate=delegate;
            this.rightRowsSeen=rightRowsSeen;
        }

        @Override public boolean hasNext(){ return delegate.hasNext(); }

        @Override
        public ExecRow next(){
            ExecRow r = delegate.next();
            if(r!=null)
                rightRowsSeen.increment();
            return r;
        }

        @Override public void remove(){ delegate.remove();}
    }
}
