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

package com.splicemachine.timestamp.impl;

import com.splicemachine.timestamp.api.TimestampIOException;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimestampQueue implements Runnable {
    private static final Logger LOG = Logger.getLogger(TimestampQueue.class);
    private final int id;
    private final int timeoutMillis;

    private BlockingQueue<Object> exchange = new SynchronousQueue(true);
    private Semaphore semaphore = new Semaphore(0, true);
    private TimestampConnection connection;

    public TimestampQueue(TimestampConnection connection, int id, int timeoutMillis) {
        this.id = id;
        this.connection = connection;
        this.timeoutMillis = timeoutMillis;
    }

    public Long get(long timeoutMillis) throws Exception {
        semaphore.release();
        Object result = exchange.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        if (result == null)
            throw new TimestampIOException("Operation timed out", new TimeoutException());
        if (result instanceof Exception)
            throw (Exception) result;
        return (Long) result;
    }

    @Override
    public void run() {
        boolean exit = false;
        while(!exit) {
            try {
                int batchSize;
                if (semaphore.availablePermits() > 0) {
                    batchSize = semaphore.drainPermits();
                } else {
                    semaphore.acquire();
                    batchSize = 1;
                }
                try {
                    long[] timestamps;
                    if (batchSize > 1) {
                        timestamps = connection.getBatchTimestamps(batchSize);
                    } else {
                        timestamps = new long[] { connection.getSingleTimestamp() };
                    }
                    for (int i = 0; i < batchSize; ++i) {
                        try {
                            if (!exchange.offer(timestamps[i], timeoutMillis, TimeUnit.MILLISECONDS)) {
                                LOG.warn("Client not waiting, discarded timestamp " + timestamps[i]);
                            }
                        } catch (InterruptedException e) {
                            LOG.error("Interrupted, exiting", e);
                            exit = true;
                            break;
                        }
                    }
                } catch (TimestampIOException e) {
                    LOG.error("Unexpected exception", e);
                    for (int i = 0; i < batchSize; ++i) {
                        try {
                            if (!exchange.offer(e, timeoutMillis, TimeUnit.MILLISECONDS)) {
                                LOG.warn("Client not waiting, couldn't notify exception");
                            }
                        } catch (InterruptedException ie) {
                            LOG.error("Interrupted, exiting", ie);
                            exit = true;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Unexpected error", e);
                // keep retrying
            }
        }
    }

    @Override
    public String toString() {
        return "TimestampQueue{" +
                "id=" + id +
                '}';
    }
}
