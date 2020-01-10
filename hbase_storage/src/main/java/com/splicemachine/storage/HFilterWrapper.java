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

import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public class HFilterWrapper implements DataFilter{
    private Filter hbaseFilter;

    public HFilterWrapper(Filter hbaseFilter){
        this.hbaseFilter=hbaseFilter;
    }

    @Override
    public ReturnCode filterCell(DataCell keyValue) throws IOException{
        /*
         * This is generally not used. The FilterWrapper class is mainly present to provide API
         * compatibility. Still, just in case it IS used, we implement it.
         */
        assert keyValue instanceof HCell: "Programmer error: incorrect type for filter!";
        org.apache.hadoop.hbase.filter.Filter.ReturnCode rc = hbaseFilter.filterKeyValue(((HCell)keyValue).unwrapDelegate());
        switch(rc){
            case NEXT_ROW:
                return ReturnCode.NEXT_ROW;
            case INCLUDE:
                return ReturnCode.INCLUDE;
            case INCLUDE_AND_NEXT_COL:
                return ReturnCode.INCLUDE_AND_NEXT_COL;
            case NEXT_COL:
                return ReturnCode.NEXT_COL;
            case SEEK_NEXT_USING_HINT:
                return ReturnCode.SEEK;
            case SKIP:
                return ReturnCode.SKIP;
            default:
                throw new IllegalStateException("Unexpected ReturnCode "+ rc+"!");
        }
    }

    @Override
    public boolean filterRow() throws IOException{
        return hbaseFilter.filterRow();
    }

    @Override
    public void reset() throws IOException{
        hbaseFilter.reset();
    }

    public Filter unwrapDelegate(){
        return hbaseFilter;
    }
}
