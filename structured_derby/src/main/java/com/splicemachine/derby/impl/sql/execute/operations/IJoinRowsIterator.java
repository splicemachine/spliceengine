package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.utils.StandardIterator;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Iterator;

/**
 * Marking interface for iterators that produce rows to be consumed by join operations. Each element
 * produced by iterator is a left row and the (possibly empty) right rows with which it may join, depending
 * on the join logic of the particular operator.
 *
 * @author P Trolard
 *         Date: 15/11/2013
 */
public interface IJoinRowsIterator<T> extends StandardIterator<Pair<T,Iterator<T>>> {

    // For debugging: return number of rows processed
    int getLeftRowsSeen();
    int getRightRowsSeen();
}
