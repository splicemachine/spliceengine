package com.splicemachine.storage;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.hadoop.hbase.Cell;

import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class CellIterable implements Iterable<DataCell>{
    private Iterable<Cell> delegate;

    private HCell wrapper = new HCell();
    private Function<Cell, DataCell> transform=new Function<Cell, DataCell>(){
        @Override
        public DataCell apply(Cell input){
            wrapper.set(input);
            return wrapper;
        }
    };
    public CellIterable(Iterable<Cell> delegate){
        this.delegate=delegate;
    }

    @Override
    public Iterator<DataCell> iterator(){
        return Iterators.transform(delegate.iterator(),transform);
    }
}
