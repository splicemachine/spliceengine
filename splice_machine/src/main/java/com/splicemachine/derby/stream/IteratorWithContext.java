package com.splicemachine.derby.stream;

import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;

import java.util.Iterator;

/**
 * Created by jleach on 4/17/15.
 */
public interface IteratorWithContext<T> extends Iterable<T>, Iterator<T> {
    void prepare();
    void reset();
    T call(T t);
}
