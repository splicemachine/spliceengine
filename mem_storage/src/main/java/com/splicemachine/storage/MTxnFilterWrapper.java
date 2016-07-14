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

import com.splicemachine.si.impl.filter.PackedTxnFilter;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public class MTxnFilterWrapper implements DataFilter{
    private final DataFilter delegate;

    public MTxnFilterWrapper(DataFilter delegate){
        this.delegate=delegate;
    }

    @Override
    public ReturnCode filterCell(DataCell keyValue) throws IOException{
        return delegate.filterCell(keyValue);
    }

    @Override
    public boolean filterRow() throws IOException{
        return delegate.filterRow();
    }

    @Override
    public void reset() throws IOException{
        delegate.reset();
    }

    public void filterRow(List<DataCell> cells){
        if(delegate instanceof PackedTxnFilter){
            cells.clear();
            DataCell e=((PackedTxnFilter)delegate).produceAccumulatedResult();
            if(e!=null)
                cells.add(e);
        }
    }
}
