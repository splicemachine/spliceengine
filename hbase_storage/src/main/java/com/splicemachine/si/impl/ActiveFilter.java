/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import org.apache.hadoop.hbase.filter.FilterBase;
import java.io.IOException;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class ActiveFilter extends FilterBase implements HasPredicateFilter{
    public TxnFilter filterState=null;

    private transient HCell wrapper = new HCell();

    public ActiveFilter(){ }

    public ActiveFilter(TxnFilter filterState){
        this.filterState=filterState;
    }

    @Override
    public EntryPredicateFilter getFilter(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public ReturnCode filterKeyValue(Cell keyValue){
        try{
            initFilterStateIfNeeded();
            wrapper.set(keyValue);
            DataFilter.ReturnCode code=filterState.filterCell(wrapper);
            switch(code){
                case NEXT_ROW:
                    return ReturnCode.NEXT_ROW;
                case INCLUDE:
                    return ReturnCode.INCLUDE;
                case INCLUDE_AND_NEXT_COL:
                    return ReturnCode.INCLUDE_AND_NEXT_COL;
                case NEXT_COL:
                    return ReturnCode.NEXT_COL;
                case SEEK:
                    return ReturnCode.SEEK_NEXT_USING_HINT;
                case SKIP:
                    return ReturnCode.SKIP;
                default:
                    throw new IllegalStateException("Unexpected Return code: "+ code);
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    public void initFilterStateIfNeeded() throws IOException{
        if(filterState==null){
            throw new UnsupportedOperationException("IMPLEMENT");
        }
    }

/*    public boolean filterRow(){
        return filterState.getExcludeRow();
    }
*/
    /*
    public boolean hasFilterRow(){
        return true;
    }
    */

    public void reset(){
        if(filterState!=null)
            filterState.nextRow();
    }

}