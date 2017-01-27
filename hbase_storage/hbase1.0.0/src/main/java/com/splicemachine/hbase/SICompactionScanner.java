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

package com.splicemachine.hbase;

import com.splicemachine.si.impl.server.SICompactionState;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Decorator for an HBase scanner that performs SI operations at compaction time. Delegates the core work to
 * SICompactionState.
 */
public class SICompactionScanner implements InternalScanner {
    private final SICompactionState compactionState;
    private final InternalScanner delegate;
    private List<Cell> rawList =new ArrayList<>();

    public SICompactionScanner(SICompactionState compactionState,
                               InternalScanner scanner) {
        this.compactionState = compactionState;
        this.delegate = scanner;
    }

    @Override
    public boolean next(List<Cell> list) throws IOException{
        /*
         * Read data from the underlying scanner and send the results through the SICompactionState.
         */
        rawList.clear();
        final boolean more = delegate.next(rawList);
        compactionState.mutate(rawList, list);
        return more;
    }

    public boolean next(List<Cell> results, int limit) throws IOException {
        return next(results);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}