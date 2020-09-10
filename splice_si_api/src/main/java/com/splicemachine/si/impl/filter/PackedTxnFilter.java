/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
            case FIRST_WRITE_TOKEN:
            case DELETE_RIGHT_AFTER_FIRST_WRITE_TOKEN:
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
