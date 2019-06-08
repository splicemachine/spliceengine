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

package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
// imports for classes still in regionserver.wal
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.wal.BoundedRegionGroupingProvider;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.RegionGroupingProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;


public class SpliceWALProvider extends RegionGroupingProvider {
    private static final Log LOG = LogFactory.getLog(org.apache.hadoop.hbase.wal.BoundedRegionGroupingProvider.class);

    static final String NUM_REGION_GROUPS = "hbase.wal.regiongrouping.numgroups";
    static final int DEFAULT_NUM_REGION_GROUPS = 2;
    private WALProvider[] delegates;
    private AtomicInteger counter = new AtomicInteger(0);
    private ConcurrentMap<String, AtomicInteger> tableCounts = new ConcurrentHashMap<>();
    private ConcurrentMap<Pair<WALProvider, String>, AtomicInteger> perWALCounts = new ConcurrentHashMap<>();

    @Override
    public void init(final WALFactory factory, final Configuration conf,
                     final List<WALActionsListener> listeners, final String providerId) throws IOException {
        super.init(factory, conf, listeners, providerId);
        // no need to check for and close down old providers; our parent class will throw on re-invoke
        delegates = new WALProvider[Math.max(1, conf.getInt(NUM_REGION_GROUPS,
                DEFAULT_NUM_REGION_GROUPS))];
        for (int i = 0; i < delegates.length; i++) {
            delegates[i] = factory.getProvider(DELEGATE_PROVIDER, DEFAULT_DELEGATE_PROVIDER, listeners,
                    providerId + i);
        }
        LOG.info("Configured to run with " + delegates.length + " delegate WAL providers.");
    }

    @Override
    synchronized WALProvider populateCache(final byte[] group) {
        try {
            String regionName = Bytes.toString(group);
            String tableName = regionName.split(",")[0];

            AtomicInteger tableCount = tableCounts.computeIfAbsent(tableName, ignored -> new AtomicInteger());
            int c = tableCount.getAndIncrement();

            int perLog = c / delegates.length;

            WALProvider temp = delegates[counter.getAndIncrement() % delegates.length];
            Pair p = Pair.newPair(temp, tableName);
            AtomicInteger walc = perWALCounts.computeIfAbsent(p, ig -> new AtomicInteger());
            while (walc.get() > perLog) {
                temp = delegates[counter.getAndIncrement() % delegates.length];
                walc = perWALCounts.computeIfAbsent(p, ig -> new AtomicInteger());
            }
            walc.incrementAndGet();
            final WALProvider extant = cached.putIfAbsent(group, temp);
            // if someone else beat us to initializing, just take what they set.
            // note that in such a case we skew load away from the provider we picked at first
            return extant == null ? temp : extant;
        } catch (Exception e) {
            LOG.warn("Error ", e);

            final WALProvider temp = delegates[counter.getAndIncrement() % delegates.length];
            final WALProvider extant = cached.putIfAbsent(group, temp);
            // if someone else beat us to initializing, just take what they set.
            // note that in such a case we skew load away from the provider we picked at first
            return extant == null ? temp : extant;
        }
    }

    @Override
    public void shutdown() throws IOException {
        // save the last exception and rethrow
        IOException failure = null;
        for (WALProvider provider : delegates) {
            try {
                provider.shutdown();
            } catch (IOException exception) {
                LOG.error("Problem shutting down provider '" + provider + "': " + exception.getMessage());
                LOG.debug("Details of problem shutting down provider '" + provider + "'", exception);
                failure = exception;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void close() throws IOException {
        // save the last exception and rethrow
        IOException failure = null;
        for (WALProvider provider : delegates) {
            try {
                provider.close();
            } catch (IOException exception) {
                LOG.error("Problem closing provider '" + provider + "': " + exception.getMessage());
                LOG.debug("Details of problem shutting down provider '" + provider + "'", exception);
                failure = exception;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * iff the given WALFactory is using the BoundedRegionGroupingProvider for meta and/or non-meta,
     * count the number of files (rolled and active). if either of them isn't, count 0
     * for that provider.
     * @param walFactory may not be null.
     */
    public static long getNumLogFiles(WALFactory walFactory) {
        long result = 0;
        if (walFactory.provider instanceof SpliceWALProvider) {
            SpliceWALProvider groupProviders =
                    (SpliceWALProvider)walFactory.provider;
            for (int i = 0; i < groupProviders.delegates.length; i++) {
                result +=
                        ((FSHLog)((DefaultWALProvider)(groupProviders.delegates[i])).log).getNumLogFiles();
            }
        }
        WALProvider meta = walFactory.metaProvider.get();
        if (meta instanceof SpliceWALProvider) {
            for (int i = 0; i < ((SpliceWALProvider)meta).delegates.length; i++) {
                result += ((FSHLog)
                        ((DefaultWALProvider)(((SpliceWALProvider)meta).delegates[i])).log)
                        .getNumLogFiles();      }
        }
        return result;
    }

    /**
     * iff the given WALFactory is using the BoundedRegionGroupingProvider for meta and/or non-meta,
     * count the size of files (rolled and active). if either of them isn't, count 0
     * for that provider.
     * @param walFactory may not be null.
     */
    public static long getLogFileSize(WALFactory walFactory) {
        long result = 0;
        if (walFactory.provider instanceof SpliceWALProvider) {
            SpliceWALProvider groupProviders =
                    (SpliceWALProvider)walFactory.provider;
            for (int i = 0; i < groupProviders.delegates.length; i++) {
                result +=
                        ((FSHLog)((DefaultWALProvider)(groupProviders.delegates[i])).log).getLogFileSize();
            }
        }
        WALProvider meta = walFactory.metaProvider.get();
        if (meta instanceof SpliceWALProvider) {
            for (int i = 0; i < ((SpliceWALProvider)meta).delegates.length; i++) {
                result += ((FSHLog)
                        ((DefaultWALProvider)(((SpliceWALProvider)meta).delegates[i])).log)
                        .getLogFileSize();
            }
        }
        return result;
    }
}
