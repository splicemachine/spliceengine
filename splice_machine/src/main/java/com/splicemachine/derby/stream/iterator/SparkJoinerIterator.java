package com.splicemachine.derby.stream.iterator;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.Joiner;
import java.util.Iterator;

/**
* Created by dgomezferro on 4/7/15.
 *
 * XXX-TODO FIX JL
*/
public class SparkJoinerIterator implements Iterator<ExecRow>, Iterable<ExecRow> {
    ExecRow next = null;
    boolean consumed = false;
    boolean iterated = false;
    private Joiner joiner;
    public SparkJoinerIterator(Joiner joiner) {
        this.joiner = joiner;
    }

    @Override
    public synchronized Iterator<ExecRow> iterator() {
        if (iterated) {
            // we assume this is only called once
            throw new IllegalStateException("Iterator has already been used");
        }
        iterated = true;
        return this;
    }

    @Override
    public boolean hasNext() {
        if (consumed) return false;
        try {
            if (next == null) {
                next = joiner.nextRow(null);
            }
            if (next == null) {
                consumed = true;
                joiner.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return next != null;
    }

    @Override
    public ExecRow next() {
        if (!hasNext())
            return null;
        ExecRow result = next;
        next = null;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Can't remove elements from this iterator");
    }
}
