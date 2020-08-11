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

package com.splicemachine.si.impl;

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

    public SIFilterPacked(){ }

    public SIFilterPacked(TxnFilter filterState){
        this.filterState=filterState;
    }

    @Override
    public EntryPredicateFilter getFilter(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Filter.ReturnCode filterKeyValue(Cell keyValue){
        try{
            initFilterStateIfNeeded();
            DataFilter.ReturnCode code=filterState.filterCell(new HCell(keyValue));
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
