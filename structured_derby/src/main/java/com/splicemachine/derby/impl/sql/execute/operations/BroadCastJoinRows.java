package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author P Trolard
 *         Date: 23/03/2014
 */
public class BroadCastJoinRows implements IJoinRowsIterator<ExecRow> {

    private static final Logger LOG = Logger.getLogger(BroadCastJoinRows.class);

    private final StandardIterator<ExecRow> leftRows;
    private final Function<ExecRow,List<ExecRow>> rightSideLookup;
    private Pair<ExecRow, Iterator<ExecRow>> pair;
    private int leftRowsSeen;
    private int rightRowsSeen;

    public BroadCastJoinRows(StandardIterator<ExecRow> leftRows,
                             Function<ExecRow,List<ExecRow>> rightSideLookup) {
        this.leftRows = leftRows;
        this.rightSideLookup = rightSideLookup;
        this.pair = new Pair<ExecRow, Iterator<ExecRow>>();
    }

    @Override
    public Pair<ExecRow, Iterator<ExecRow>> next(SpliceRuntimeContext ctx)
            throws StandardException, IOException {
        ExecRow left = leftRows.next(ctx);
        if (left != null){
            leftRowsSeen++;
            List<ExecRow> rights = rightSideLookup.apply(left);
            if (rights == null){
                rights = Collections.EMPTY_LIST;
            }
            // TODO don't count right rows already seen
            rightRowsSeen += rights.size();
            pair.setFirst(left);
            pair.setSecond(rights.iterator());
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
        leftRows.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        leftRows.close();
        pair = null;
    }
}
