package com.splicemachine.si.impl.filter;

import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.si.api.filter.SIFilter;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;

import java.io.IOException;

public class PackedTxnFilter implements TxnFilter, SIFilter{
    protected final TxnFilter simpleFilter;
    public final RowAccumulator accumulator;
    private DataCell lastValidCell;
    protected boolean excludeRow=false;

    public PackedTxnFilter(TxnFilter simpleFilter,RowAccumulator accumulator){
        this.simpleFilter=simpleFilter;
        this.accumulator=accumulator;
    }

    public RowAccumulator getAccumulator(){
        return accumulator;
    }

    @Override
    public void reset(){
       nextRow();
    }

    @Override
    public DataFilter.ReturnCode filterCell(DataCell keyValue) throws IOException{
        final DataFilter.ReturnCode returnCode=simpleFilter.filterCell(keyValue);
        switch(keyValue.dataType()){
            case COMMIT_TIMESTAMP:
                return returnCode; // These are always skip...
            case USER_DATA:
            return processUserData(keyValue,returnCode);
            case TOMBSTONE:
            case ANTI_TOMBSTONE:
            case FOREIGN_KEY_COUNTER:
            case OTHER:
                return returnCode; // These are always skip...

            default:
                throw new RuntimeException("unknown key value type");
        }
    }

    @Override
    public boolean filterRow(){
        return getExcludeRow();
    }

    private DataFilter.ReturnCode processUserData(DataCell keyValue,DataFilter.ReturnCode returnCode) throws IOException{
        switch(returnCode){
            case INCLUDE:
            case INCLUDE_AND_NEXT_COL:
                return accumulate(keyValue);
            case NEXT_COL:
            case SKIP:
            case NEXT_ROW:
                return DataFilter.ReturnCode.SKIP;
            default:
                throw new RuntimeException("Unknown return code");
        }
    }

    @Override
    public DataCell produceAccumulatedResult(){
        if(excludeRow) return null;
        if(accumulator.isCountStar())
            return lastValidCell;
        if(lastValidCell==null)
            return null;
        final byte[] resultData=accumulator.result();
        if(resultData!=null){
            return lastValidCell.copyValue(resultData);
        }else{
            return null;
        }
    }

    public DataFilter.ReturnCode accumulate(DataCell data) throws IOException{
        if(!accumulator.isFinished() && !excludeRow && accumulator.isInteresting(data)){
            if(!accumulator.accumulateCell(data)){
                excludeRow=true;
            }
        }
        if(lastValidCell==null){
            lastValidCell=data;
            return DataFilter.ReturnCode.INCLUDE;
        }else
            return DataFilter.ReturnCode.SKIP;
    }

    @Override
    public boolean getExcludeRow(){
        return excludeRow || lastValidCell==null;
    }

    @Override
    public void nextRow(){
        simpleFilter.nextRow();
        accumulator.reset();
        lastValidCell=null;
        excludeRow=false;
    }

    @Override
    public DataStore getDataStore(){
        return simpleFilter.getDataStore();
    }

}
