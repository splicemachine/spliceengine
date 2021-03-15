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
package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An In-Memory store that does not keep track of the procedures inserted.
 */
@InterfaceAudience.Private
public class SpliceNoopProcedureStore extends WALProcedureStore {
    private int numThreads;

    public SpliceNoopProcedureStore(Configuration conf, LeaseRecovery leaseRecovery) throws IOException {
        super(conf, leaseRecovery);
    }

    @Override
    public void start(int numThreads) throws IOException {
        if (!setRunning(true)) {
            return;
        }
        this.numThreads = numThreads;
    }

    @Override
    public void stop(boolean abort) {
        setRunning(false);
    }

    @Override
    public void recoverLease() throws IOException {
        // no-op
    }

    @Override
    public int getNumThreads() {
        return numThreads;
    }

    @Override
    public int setRunningProcedureCount(final int count) {
        return count;
    }

    @Override
    public void load(final ProcedureLoader loader) throws IOException {
        loader.setMaxProcId(0);
    }

    @Override
    public void insert(Procedure<?> proc, Procedure<?>[] subprocs) {
        // no-op
    }

    @Override
    public void insert(Procedure<?>[] proc) {
        // no-op
    }

    @Override
    public void update(Procedure<?> proc) {
        // no-op
    }

    @Override
    public void delete(long procId) {
        // no-op
    }

    @Override
    public void delete(Procedure<?> proc, long[] subprocs) {
        // no-op
    }

    @Override
    public void delete(long[] procIds, int offset, int count) {
        // no-op
    }
}
