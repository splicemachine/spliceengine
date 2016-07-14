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
