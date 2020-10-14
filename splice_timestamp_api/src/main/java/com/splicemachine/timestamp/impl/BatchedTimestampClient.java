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

import com.splicemachine.timestamp.api.TimestampHostProvider;
import com.splicemachine.timestamp.api.TimestampIOException;
import org.apache.log4j.Logger;

import java.util.Random;

/**
 * Same as {@link TimestampClient} but batches new timestamps requests for better throughput
 * (sacrificing latency)

 * Requests for new timestamps (the most common call) get batched up into N different queues  ({@link TimestampQueue}).
 * Each of the queues has a single outstanding request at a time. When a new request comes in, it chooses one queue at
 * random, if the queue is empty it will serve the request right away, if it's busy the request will be
 * batched and served as soon as the queue completes the ongoing request.
 *
 */
public class BatchedTimestampClient extends TimestampClient {

    private static final Logger LOG = Logger.getLogger(BatchedTimestampClient.class);

    private final TimestampQueue queues[];
    private final Thread queueThreads[];
    private final Random rand = new Random();

    public BatchedTimestampClient(int timeoutMillis, TimestampHostProvider timestampHostProvider, int queues) {
        super(timeoutMillis, timestampHostProvider);

        this.queues = new TimestampQueue[queues];
        this.queueThreads = new Thread[queues];

    }

    public void start() {
        for (int i = 0; i < queues.length; ++i) {
            queues[i] = new TimestampQueue(connection, i, timeoutMillis);
            queueThreads[i] = new Thread(queues[i], "TimestampClient-queue"+i);
            queueThreads[i].setDaemon(true);
            queueThreads[i].start();
        }
    }

    public long getNextTimestamp() throws TimestampIOException {
        int idx = rand.nextInt(queues.length);
        try {
            Long res = queues[idx].get(timeoutMillis);
            return res;
        } catch (TimestampIOException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unexpected exception", e);
            throw new TimestampIOException("Couldn't request batch", e);
        }
    }
}
