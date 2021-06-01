/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.stream;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * If the cache mode is enabled, it starts one thread to check the timeout on message consumption and
 * if the timeout occurs, the client does not consume messages, it starts to cache messages from Spark to the file
 * system.
 */
public class MessageBackupHandler {
    private static final Logger LOG = Logger.getLogger(MessageBackupHandler.class);

    static final int CONSUMPTION_TIMEOUT = 300000;

    private UUID uuid;
    private HdfsStreamWriter hdfsStreamWriter;
    private ScheduledExecutorService timeoutMonitorExecutor;
    private ExecutorService backupExecutor;

    public MessageBackupHandler(UUID uuid, StreamListener  streamListener) {
        this.uuid = uuid;
        this.hdfsStreamWriter = new HdfsStreamWriter(uuid);
        this.timeoutMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
        this.timeoutMonitorExecutor.scheduleAtFixedRate(new ConsumptionTimeoutTask(streamListener, CONSUMPTION_TIMEOUT), 0, 1, TimeUnit.MINUTES);
        this.backupExecutor = Executors.newFixedThreadPool(2);
    }

    public void submitBackupTask(BackupPartitionTask task) {
        backupExecutor.submit(task);
    }

    public void writeResults(int partition, Queue<Object> queue) throws IOException {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("Write messages of partition %s/%d to file", uuid.toString(), partition));
        hdfsStreamWriter.writeResults(partition, queue);
    }

    public ArrayBlockingQueue<Object> readResults(int partition) throws IOException {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("Load messages of partition %s/%d from file", uuid.toString(), partition));
        return hdfsStreamWriter.readResults(partition);
    }

    public void close() {
        if (this.timeoutMonitorExecutor != null) {
            this.timeoutMonitorExecutor.shutdown();
        }
        if (this.backupExecutor != null) {
            this.backupExecutor.shutdown();
        }
        if (this.hdfsStreamWriter != null) {
            this.hdfsStreamWriter.close();
        }
    }
}

enum ResultStorage {MEMORY, FILE, WRITING}

class ConsumptionTimeoutTask implements Runnable {
    private final StreamListener streamListener;
    private final long timeout;

    ConsumptionTimeoutTask(StreamListener streamListener, long timeout) {
        this.streamListener = streamListener;
        this.timeout = timeout;
    }

    @Override
    public void run() {
        if (streamListener.getLastReadTime() != null && streamListener.getLastReadTime() + this.timeout < System.currentTimeMillis()) {
            if (!streamListener.isBackupEnabled()) {
                streamListener.setBackupEnabled(true);
            }
        } else {
            if (streamListener.isBackupEnabled()) {
                streamListener.setBackupEnabled(false);
            }
        }
    }
}

class BackupPartitionTask implements Runnable {
    private final StreamListener streamListener;
    private final int partition;

    BackupPartitionTask(StreamListener streamListener, int partition) {
        this.streamListener = streamListener;
        this.partition = partition;
    }

    @Override
    public void run() {
        streamListener.backupPartition(partition);
    }
}


