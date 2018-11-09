/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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

import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.ByteSlice;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public class HBaseRollForward implements Runnable, RollForward {
    private static final Logger LOG = Logger.getLogger(HBaseRollForward.class);

    private ArrayBlockingQueue<RFEvent> queue;
    private Thread rollforwardThread;
    private TxnSupplier supplier;

    public HBaseRollForward(TxnSupplier supplier) {
        this.queue = new ArrayBlockingQueue<>(4096);

        this.rollforwardThread = new Thread(this,"WritesRollforwarder");
        this.rollforwardThread.setDaemon(true);
        this.supplier = supplier;
    }

    public void start() {
        rollforwardThread.start();
    }

    @Override
    public void submitForResolution(Partition partition, ByteSlice rowKey, long txnId) {
        queue.offer(new RFEvent(partition, rowKey, txnId, System.currentTimeMillis()));
    }

    @Override
    public void run() {
        while(true) {
            try {
                RFEvent event = queue.take();
                long current = System.currentTimeMillis();
                long diff = current - event.getTimestamp();
                if (diff < 1000) {
                    Thread.sleep(1000-diff);
                }

                TxnView txn = supplier.getTransaction(event.getTxnId());
                Txn.State state = txn.getEffectiveState();
                if(state.isFinal()) {
                    if (state == Txn.State.ROLLEDBACK) {
                        SynchronousReadResolver.INSTANCE.resolveRolledback(
                                event.getPartition(), event.getByteSlice(), event.getTxnId(), false);
                    } else if (state == Txn.State.COMMITTED) {
                        SynchronousReadResolver.INSTANCE.resolveCommitted(
                                event.getPartition(), event.getByteSlice(), event.getTxnId(), txn.getEffectiveCommitTimestamp(), false);
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
