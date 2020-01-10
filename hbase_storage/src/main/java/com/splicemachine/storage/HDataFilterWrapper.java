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

package com.splicemachine.storage;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/2/16
 */
public class HDataFilterWrapper extends FilterBase{
    private DataFilter dataFilter;
    private HCell cell = new HCell();

    public HDataFilterWrapper(DataFilter dataFilter){
        this.dataFilter=dataFilter;
    }

    @Override
    public void reset() throws IOException{
       dataFilter.reset();
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException{
        cell.set(v);
        DataFilter.ReturnCode returnCode=dataFilter.filterCell(cell);
        switch(returnCode){
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
                throw new IllegalArgumentException("Unexpected return code: "+ returnCode);
        }
    }

    @Override
    public boolean hasFilterRow(){
        return true;
    }

    @Override
    public boolean filterRow() throws IOException{
        return dataFilter.filterRow();
    }


    @Override
    public byte[] toByteArray() throws IOException{
        throw new UnsupportedOperationException("Serialization not supported");
    }
}
