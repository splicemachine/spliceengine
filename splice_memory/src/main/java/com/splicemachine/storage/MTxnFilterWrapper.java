package com.splicemachine.storage;

import com.splicemachine.si.impl.filter.PackedTxnFilter;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public class MTxnFilterWrapper implements DataFilter{
    private final DataFilter delegate;

    public MTxnFilterWrapper(DataFilter delegate){
        this.delegate=delegate;
    }

    @Override
    public ReturnCode filterKeyValue(DataCell keyValue) throws IOException{
        return delegate.filterKeyValue(keyValue);
    }

    @Override
    public boolean filterRow(){
        return delegate.filterRow();
    }

    @Override
    public void reset(){
        delegate.reset();
    }

    public void filterRow(List<DataCell> cells){
        if(delegate instanceof PackedTxnFilter){
            cells.clear();
            DataCell e=((PackedTxnFilter)delegate).produceAccumulatedResult();
            if(e!=null)
                cells.add(e);
        }
    }
}
