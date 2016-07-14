/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.impl.filter;

import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.si.api.filter.SIFilter;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.storage.CellType;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;

import java.io.IOException;

public class PackedTxnFilter implements TxnFilter, SIFilter{
    private final TxnFilter simpleFilter;
    public final RowAccumulator accumulator;
    private DataCell lastValidCell;
    private boolean excludeRow=false;

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
            return lastValidCell.copyValue(resultData,CellType.USER_DATA);
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

}
