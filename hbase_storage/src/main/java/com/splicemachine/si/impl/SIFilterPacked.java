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

package com.splicemachine.si.impl;

import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.storage.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.IOException;
import java.util.List;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilterPacked extends FilterBase implements HasPredicateFilter{
    public TxnFilter filterState=null;

    private transient HCell wrapper = new HCell();

    public SIFilterPacked(){ }

    public SIFilterPacked(TxnFilter filterState){
        this.filterState=filterState;
    }

    @Override
    public long getBytesVisited(){
        if(filterState==null) return 0l;
        RowAccumulator accumulator=filterState.getAccumulator();
        return accumulator.getBytesVisited();
    }

    @Override
    public EntryPredicateFilter getFilter(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Filter.ReturnCode filterKeyValue(Cell keyValue){
        try{
            initFilterStateIfNeeded();
            wrapper.set(keyValue);
            DataFilter.ReturnCode code=filterState.filterCell(wrapper);
            switch(code){
                case NEXT_ROW:
                    return Filter.ReturnCode.NEXT_ROW;
                case INCLUDE:
                    return Filter.ReturnCode.INCLUDE;
                case INCLUDE_AND_NEXT_COL:
                    return Filter.ReturnCode.INCLUDE_AND_NEXT_COL;
                case NEXT_COL:
                    return Filter.ReturnCode.NEXT_COL;
                case SEEK:
                    return Filter.ReturnCode.SEEK_NEXT_USING_HINT;
                case SKIP:
                    return Filter.ReturnCode.SKIP;
                default:
                    throw new IllegalStateException("Unexpected Return code: "+ code);
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void filterRowCells(List<Cell> keyValues){
        // FIXME: this is scary
        try{
            initFilterStateIfNeeded();
        }catch(IOException e){
            throw new RuntimeException(e);
        }
        if(!filterRow())
            keyValues.remove(0);
        //TODO -sf- figure out a way to remove the DataCell object here
        DataCell accumulatedResults = filterState.produceAccumulatedResult();
        if(accumulatedResults!=null){
            keyValues.add(((HCell)accumulatedResults).unwrapDelegate());
        }
    }

    public void initFilterStateIfNeeded() throws IOException{
        if(filterState==null){
            throw new UnsupportedOperationException("IMPLEMENT");
        }
    }

    public boolean filterRow(){
        return filterState.getExcludeRow();
    }

    public boolean hasFilterRow(){
        return true;
    }

    public void reset(){
        if(filterState!=null)
            filterState.nextRow();
    }

}