package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.sql.execute.ExecRow;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Created by jleach on 6/15/17.
 */
public class ClonedIterator implements Iterator {
    private Iterator<ExecRow> locatedRowIterator;

    public ClonedIterator(Iterator locatedRowIterator) {
        this.locatedRowIterator = locatedRowIterator;
    }

    @Override
    public boolean hasNext() {
        return locatedRowIterator.hasNext();
    }

    @Override
    public Object next() {
        return locatedRowIterator.next().getClone();
    }

    @Override
    public void remove() {
        locatedRowIterator.remove();
    }

    @Override
    public void forEachRemaining(Consumer action) {
        locatedRowIterator.forEachRemaining(action);
    }
}
