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

package com.splicemachine.si.impl.rollforward;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Date: 9/4/14
 */
public class RollForwardStatus implements RollForwardManagement{
    private final AtomicLong numUpdates = new AtomicLong(0l);
    private final AtomicLong rowsToResolve = new AtomicLong(0l);

    @Override public long getTotalUpdates() { return numUpdates.get(); }
    @Override public long getTotalRowsToResolve() { return  rowsToResolve.get(); }

    public void rowResolved(){
        boolean shouldContinue;
        do{
            long curr = rowsToResolve.get();
            if(curr<=0) return; //we didn't record this row, but we DID resolve it
            shouldContinue = !rowsToResolve.compareAndSet(curr,curr-1);
        }while(shouldContinue);
    }

    public void rowWritten(){
        rowsToResolve.incrementAndGet();
        numUpdates.incrementAndGet();
    }
}
