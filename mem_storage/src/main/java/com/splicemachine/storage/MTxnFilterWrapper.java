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
