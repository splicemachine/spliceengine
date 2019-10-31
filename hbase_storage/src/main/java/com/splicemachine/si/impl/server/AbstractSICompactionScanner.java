/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.si.impl.server;

import com.splicemachine.si.api.txn.TxnView;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Decorator for an HBase scanner that performs SI operations at compaction time. Delegates the core work to
 * SICompactionState.
 */
public abstract class AbstractSICompactionScanner implements InternalScanner {
    private static final Logger LOG = Logger.getLogger(AbstractSICompactionScanner.class);
    private final SICompactionState compactionState;
    private final InternalScanner delegate;
    private final BlockingQueue<Entry> queue;
    private final Timer timer;
    private final int timeDelta;
    private final CompactionContext context;
    private final EnumSet<PurgeConfig> purgeConfig;
    private AtomicReference<IOException> failure = new AtomicReference<>();
    private AtomicLong remainingTime;

    public AbstractSICompactionScanner(SICompactionState compactionState,
                                       InternalScanner scanner,
                                       EnumSet<PurgeConfig> purgeConfig,
                                       double resolutionShare,
                                       int bufferSize,
                                       CompactionContext context) {
        this.compactionState = compactionState;
        this.delegate = scanner;
        this.purgeConfig = purgeConfig;
        this.queue = new ArrayBlockingQueue(bufferSize);
        this.timer = new Timer("Compaction-resolution-throttle", true);
        this.timeDelta = (int) (60000 * resolutionShare);
        this.remainingTime = new AtomicLong(timeDelta);
        this.context = context;
    }

    @Override
    public boolean next(List<Cell> list) throws IOException{
        if (failure.get() != null) {
            timer.cancel();
            throw failure.get();
        }
        /*
         * Read data from the underlying scanner and send the results through the SICompactionState.
         */
        Entry entry;
        try {
            entry = queue.take();
            final boolean more = entry.more;
            List<TxnView> txns = waitFor(entry.txns);
            compactionState.mutate(entry.cells, txns, list, purgeConfig);
            if (!more) {
                timer.cancel();
                    context.close();
            }
            return more;
        } catch (Exception e) {
            timer.cancel();
            throw new IOException(e);
        }
    }

    private List<TxnView> waitFor(List<Future<TxnView>> txns) throws ExecutionException, InterruptedException {
        List<TxnView> results = new ArrayList<>(txns.size());
        for (Future<TxnView> txn : txns) {
            if (txn == null) {
                results.add(null);
                continue;
            }

            TxnView result = null;
            long timeout = remainingTime.get();
            if (timeout < 0)
                timeout = 0;
            long start = System.currentTimeMillis();
            try {
                result = txn.get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                context.recordTimeout();
            }
            long duration = System.currentTimeMillis() - start;
            if (duration < 0)
                duration = 0;
            remainingTime.addAndGet(-duration);
            context.timeBlocked(duration);
            if (result == null) {
                context.recordUnresolvedTransaction();
            }
            results.add(result);
            if (result != null) {
                compactionState.remove(result.getTxnId());
            }
        }
        return results;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    public void start() {
        Thread thread = new Thread( new Runnable() {
            @Override
            public void run() {
                boolean more = true;
                try {
                    while (more) {
                        List<Cell> list = new ArrayList<>();
                        more = delegate.next(list);
                        List<Future<TxnView>> txns = compactionState.resolve(list);
                        queue.put(new Entry(list, txns, more));
                    }
                } catch (IOException e) {
                    LOG.error("Unexpected exception", e);
                    failure.set(e);
                } catch (InterruptedException ie) {
                    LOG.error("Unexpected exception", ie);
                    failure.set(new IOException("Compaction interrupted", ie));
                }
            }
        }, "CompactionReader");
        thread.setDaemon(true);
        thread.start();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long currentTime = remainingTime.get();
                if (currentTime < 0) {
                    remainingTime.set(timeDelta);
                } else {
                    remainingTime.addAndGet(timeDelta);
                }
            }
        }, 60000, 60000);
    }

    private static class Entry {
        List<Cell> cells;
        List<Future<TxnView>> txns;
        boolean more;

        public Entry(List<Cell> cells, List<Future<TxnView>> txns, boolean more) {
            this.cells = cells;
            this.txns = txns;
            this.more = more;
        }
    }
}
