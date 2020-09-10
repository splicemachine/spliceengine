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
 *
 */

package com.splicemachine.si.data.hbase.rollforward;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.si.api.rollforward.RollForward;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.ByteSlice;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import splice.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;

public class HBaseRollForward implements RollForward {
    private static final Logger LOG = Logger.getLogger(HBaseRollForward.class);

    private final ArrayBlockingQueue<RFEvent> firstQueue;
    private final ArrayBlockingQueue<RFEvent> secondQueue;
    private final ExecutorService serviceFirst;
    private final ExecutorService serviceSecond;
    private final TxnSupplier supplier;
    private final int firstThreads;
    private final int secondThreads;
    private final RollForwarder firstProcessor;
    private final RollForwarder secondProcessor;

    private final LongAdder firstResolutions;
    private final LongAdder firstActive;
    private final LongAdder secondResolutions;
    private final LongAdder secondActive;


    public HBaseRollForward(TxnSupplier supplier, SConfiguration config) {

        this.firstQueue = new ArrayBlockingQueue<>(config.getRollforwardQueueSize());
        this.secondQueue = new ArrayBlockingQueue<>(config.getRollforwardQueueSize());
        this.firstThreads = config.getRollforwardFirstThreads();
        this.secondThreads = config.getRollforwardSecondThreads();

        this.serviceFirst = Executors.newFixedThreadPool(firstThreads,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("WritesRollforward-%d").build());
        this.serviceSecond = Executors.newFixedThreadPool(secondThreads,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("WritesRollforward-retry-%d").build());
        this.supplier = supplier;
        this.firstResolutions = new LongAdder();
        this.secondResolutions = new LongAdder();
        this.firstActive = new LongAdder();
        this.secondActive = new LongAdder();
        this.firstProcessor = new RollForwarder(firstQueue, secondQueue, config.getRollforwardFirstWait(), firstResolutions, firstActive);
        this.secondProcessor = new RollForwarder(secondQueue, null, config.getRollforwardSecondWait(), secondResolutions, secondActive);
    }

    public void start() {
        for (int i = 0; i < firstThreads; ++i) {
            this.serviceFirst.submit(firstProcessor);
        }
        for (int i = 0; i < secondThreads; ++i) {
            this.serviceSecond.submit(secondProcessor);
        }
    }

    @Override
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE", justification = "DB-9844")
    public void submitForResolution(Partition partition, long txnId, List<ByteSlice> rowKeys) {
        firstQueue.offer(new RFEvent(partition, rowKeys, txnId, System.currentTimeMillis()));
    }


    private class RollForwarder implements Runnable{
        private BlockingQueue<RFEvent> in;
        private BlockingQueue<RFEvent> out;
        private int timeout;
        private LongAdder resolutions;
        private LongAdder active;

        RollForwarder(BlockingQueue<RFEvent> in, BlockingQueue<RFEvent> out, int timeout, LongAdder resolutions, LongAdder active) {
            this.in = in;
            this.out = out;
            this.timeout = timeout;
            this.resolutions = resolutions;
            this.active = active;
        }

        @Override
        @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE", justification = "DB-9844")
        public void run() {
            while(true) {
                try {
                    RFEvent event = in.take();
                    long current = System.currentTimeMillis();
                    long diff = current - event.getTimestamp();
                    if (diff < timeout) {
                        Thread.sleep(timeout - diff);
                    }

                    TxnView txn = supplier.getTransaction(event.getTxnId());
                    Txn.State state = txn.getEffectiveState();
                    if(state.isFinal()) {
                        resolutions.add(event.getKeys().size());
                        if (state == Txn.State.ROLLEDBACK) {
                            for (ByteSlice bs : event.getKeys()) {
                                SynchronousReadResolver.INSTANCE.resolveRolledback(
                                        event.getPartition(), bs, event.getTxnId(), false);
                            }
                        } else if (state == Txn.State.COMMITTED) {
                            for (ByteSlice bs : event.getKeys()) {
                                SynchronousReadResolver.INSTANCE.resolveCommitted(
                                        event.getPartition(), bs, event.getTxnId(), txn.getEffectiveCommitTimestamp(), false);
                            }
                        }
                    } else {
                        active.add(event.getKeys().size());
                        if (out != null) {
                            out.offer(event);
                        }
                    }
                } catch (IOException e) {
                    LOG.warn("Error while trying to roll forward writes", e);
                } catch (InterruptedException e) {
                    // Interrupted, we should exit
                    LOG.error("Interrupted, stopping roll foward thread", e);
                    break;
                }
            }
        }
    }


    @Override
    public int getFirstQueueSize() {
        return firstQueue.size();
    }

    @Override
    public int getSecondQueueSize() {
        return secondQueue.size();
    }


    @Override
    public long getFirstQueueResolutions() {
        return firstResolutions.longValue();
    }

    @Override
    public long getSecondQueueResolutions() {
        return secondResolutions.longValue();
    }

    @Override
    public long getFirstQueueActive() {
        return firstActive.longValue();
    }

    @Override
    public long getSecondQueueActive() {
        return secondActive.longValue();
    }
}
