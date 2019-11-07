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
 */

package com.splicemachine.hbase;

import com.splicemachine.access.api.GetOldestActiveTransactionTask;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionsWatcher {
    private static final Logger LOG = Logger.getLogger(TransactionsWatcher.class);
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static AtomicLong oldestActiveTransaction = new AtomicLong(Long.MAX_VALUE);

    public static TransactionsWatcher INSTANCE = new TransactionsWatcher();

    private static ScheduledExecutorService updateService =
            MoreExecutors.namedSingleThreadScheduledExecutor("hbase-transactions-watcher-%d");

    private static final Runnable updater = () -> {
        oldestActiveTransaction.set(fetchOldestActiveTransaction());
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Oldest Active transaction fetched: %d", oldestActiveTransaction.get()));
        }
    };

    private TransactionsWatcher(){}

    public void stopWatching(){
        updateService.shutdown();
    }

    /**
     * Start updating in background every UPDATE_MULTIPLE multiples
     * of update running time
     */
    public void startWatching() {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"start attempted");
        if (started.compareAndSet(false, true)) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"update service scheduled");

            SConfiguration configuration=SIDriver.driver().getConfiguration();
            long updateInterval = configuration.getTransactionsWatcherUpdateInterval();
            updateService.scheduleAtFixedRate(updater,0l,updateInterval,TimeUnit.SECONDS);
        }
    }

    // Fetching

    private static long fetchOldestActiveTransaction() {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "fetch oldest active transaction");
        long oldestActiveTransaction = Long.MAX_VALUE;
        try {
            PartitionAdmin pa = SIDriver.driver().getTableFactory().getAdmin();
            ExecutorService executorService = SIDriver.driver().getExecutorService();
            Collection<PartitionServer> servers = pa.allServers();

            List<Future<Long>> futures = Lists.newArrayList();
            for (PartitionServer server : servers) {
                GetOldestActiveTransactionTask task = SIDriver.driver().getOldestActiveTransactionTaskFactory().get(
                        server.getHostname(), server.getPort(), server.getStartupTimestamp());
                futures.add(executorService.submit(task));
            }
            for (Future<Long> future : futures) {
                long localOldestActive = future.get();
                if (localOldestActive < oldestActiveTransaction)
                    oldestActiveTransaction = localOldestActive;
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            SpliceLogUtils.error(LOG, "Unable to fetch oldestActiveTransaction", e);
        }
        return oldestActiveTransaction;
    }

    private static long getOldestActiveTransaction() {
        return oldestActiveTransaction.get();
    }
}
