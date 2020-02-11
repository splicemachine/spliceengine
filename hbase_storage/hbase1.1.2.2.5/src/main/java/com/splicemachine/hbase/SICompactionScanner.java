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

package com.splicemachine.hbase;

import com.splicemachine.si.impl.server.AbstractSICompactionScanner;
import com.splicemachine.si.impl.server.CompactionContext;
import com.splicemachine.si.impl.server.PurgeConfig;
import com.splicemachine.si.impl.server.SICompactionState;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Decorator for an HBase scanner that performs SI operations at compaction time. Delegates the core work to
 * SICompactionState.
 */
public class SICompactionScanner extends AbstractSICompactionScanner {
    private static final Logger LOG = Logger.getLogger(SICompactionScanner.class);

    public SICompactionScanner(SICompactionState compactionState,
                               InternalScanner scanner,
                               PurgeConfig purgeConfig,
                               double resolutionShare,
                               int bufferSize,
                               CompactionContext context) {
        super(compactionState, scanner, purgeConfig, resolutionShare, bufferSize, context);
    }


    public boolean next(List<Cell> results, int limit) throws IOException {
        return next(results);
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result);
    }
}
