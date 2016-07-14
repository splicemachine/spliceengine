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
