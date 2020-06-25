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

package com.splicemachine.mrio.api.core;

import com.google.common.collect.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.spark.FetchSplitsJob;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.HScan;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionLoad;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.lang.String.format;

/**
 * Created by dgomezferro on 11/30/15.
 */
public abstract class AbstractSMInputFormat<K,V> extends InputFormat<K, V> implements Configurable {
    protected static final Logger LOG = Logger.getLogger(AbstractSMInputFormat.class);
    private static int MAX_RETRIES = 30;
    private static int PARTITION_LOAD_REFRESH_THRESHOLD = 8;
    protected Configuration conf;
    protected Table table;
    private List<InputSplit> inputSplits;

    private List<InputSplit> toSMSplits (List<Partition> splits) throws IOException {
        List<InputSplit> sMSplits = Lists.newArrayList();
        HBaseTableInfoFactory infoFactory = HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration());
        for(Partition split:splits) {
            SMSplit smSplit = new SMSplit(
                    new TableSplit(
                            infoFactory.getTableInfo(split.getTableName()),
                            split.getStartKey(),
                            split.getEndKey(),
                            split.owningServer().getHostname()));
            sMSplits.add(smSplit);
        }
        return sMSplits;
    }

    private List<InputSplit> getInputSplitsFromCache(JobContext context) {
        if (inputSplits != null) {
            return inputSplits;
        }

        String splitCacheId = context.getConfiguration().get(MRConstants.SPLICE_SCAN_INPUT_SPLITS_ID);
        if (StringUtils.isNotEmpty(splitCacheId)) {
            if (FetchSplitsJob.splitCache.containsKey(splitCacheId)) {
                Future<List<InputSplit>> cachedSplitsFuture = FetchSplitsJob.splitCache.get(splitCacheId);
                List<InputSplit> cachedSplits = null;
                if (cachedSplitsFuture != null) {
                    try {
                        cachedSplits = cachedSplitsFuture.get();
                    } catch (ExecutionException | InterruptedException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
                FetchSplitsJob.splitCache.remove(splitCacheId);
                if (cachedSplits != null) {
                    inputSplits = cachedSplits;
                    return cachedSplits;
                }
            }
        }

        return null;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
            InterruptedException {

        List<InputSplit> cachedSplits = getInputSplitsFromCache(context);
        if (cachedSplits != null) {
            return cachedSplits;
        }

        setConf(context.getConfiguration());
        int splitsPerTable = conf.getInt(MRConstants.SPLICE_SPLITS_PER_TABLE, 0);

        Scan s;
        try {
            TableScannerBuilder tsb = TableScannerBuilder.getTableScannerBuilderFromBase64String(conf.get(MRConstants.SPLICE_SCAN_INFO));
            s = ((HScan)tsb.getScan()).unwrapDelegate();
        } catch (StandardException e) {
            SpliceLogUtils.error(LOG, e);
            throw new IOException(e);
        }
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getSplits with context={%s}, scan={%s}",context,s);
        SIDriver driver = SIDriver.driver();
        HBaseConnectionFactory instance = HBaseConnectionFactory.getInstance(driver.getConfiguration());
        Clock clock = driver.getClock();
        Connection connection = instance.getConnection();
        Partition clientPartition = new ClientPartition(connection, table.getName(), table, clock, driver.getPartitionInfoCache());
        int retryCounter = 0;
        boolean refresh = false;

        boolean eachRegionOneSplit = oneSplitPerRegion(conf);
        long tableSize = 0;
        while (true) {
            if (eachRegionOneSplit)
            {
                /* unconditionally refresh the region info cache for stats collection */
                List<Partition> splits = clientPartition.subPartitions(s.getStartRow(), s.getStopRow(), true);
                List<InputSplit> regionSplits = toSMSplits(splits);
                return regionSplits;
            }
            List<Partition> splits = clientPartition.subPartitions(s.getStartRow(), s.getStopRow(), refresh);
            if (splitsPerTable > 0) { // we only use the total table size if the user explicitly request a number of splits
                try {
                    String tableName = table.getName().getNameAsString().split(":")[1];
                    HBaseRegionLoads loadWatcher = HBaseRegionLoads.INSTANCE;
                    boolean refreshPartitionLoad = splits.size() < PARTITION_LOAD_REFRESH_THRESHOLD;
                    Collection<PartitionLoad> tableLoad = loadWatcher.tableLoad(tableName, refreshPartitionLoad);
                    for (PartitionLoad partitionLoad : tableLoad) {
                        tableSize += (partitionLoad.getStorefileSize() + partitionLoad.getMemStoreSize());
                    }
                    if (tableSize < 0)
                        tableSize = 0;
                }
                catch (Exception e) {
                    // Don't cause the query to abort if we couldn't get the table size.
                    // Just log a warning.
                    LOG.warn(format("Failed to compute size for table: %s", table));
                }
            }
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "getSplits " + splits);
                for (Partition split : splits) {
                    SpliceLogUtils.debug(LOG, "split -> " + split);
                }
            }
            SubregionSplitter splitter = new HBaseSubregionSplitter();

            List<InputSplit> lss= splitter.getSubSplits(table, splits, s.getStartRow(), s.getStopRow(), splitsPerTable, tableSize);
            //check if split count changed in-between

            if (isRefreshNeeded(lss)) {
                // retry
                refresh = true;
                LOG.warn("mismatched splits for region " + clientPartition + ", refresh of splits is needed");
                retryCounter++;
                if (retryCounter > MAX_RETRIES) {
                    throw new RuntimeException("MAX_RETRIES exceeded during getSplits");
                }
            } else {
                LOG.info("Splits: " + lss);
               return lss;
            }
        }
    }

    /**
     * Checks the sequence of split rows. If any gap is found, the splitting has to be recalculated again.
     *
     * @param lss
     * @return
     */
    private boolean isRefreshNeeded(List<InputSplit> lss) {
        if (lss.size() < 2 || !lss.stream().allMatch(is -> (is instanceof SMSplit))) {
            return false;
        }

        lss.sort(new Comparator<InputSplit>() {
            @Override
            public int compare(InputSplit o1, InputSplit o2) {
                SMSplit smSplit1 = (SMSplit) o1;
                SMSplit smSplit2 = (SMSplit) o2;

                return org.apache.hadoop.hbase.util.Bytes.compareTo(smSplit1.split.getStartRow(), smSplit2.split.getStartRow());
            }
        });

        for (int i = 1; i < lss.size(); i++) {
            byte currentStartRow[] = ((SMSplit) lss.get(i)).split.getStartRow();
            byte prevEndRow[] = ((SMSplit) lss.get(i - 1)).split.getEndRow();
            if (org.apache.hadoop.hbase.util.Bytes.compareTo(currentStartRow, prevEndRow) != 0) {
                LOG.warn("The gap in splits is found: current split [" + lss.get(i) + "], previous split [" + lss.get(i - 1) + "]");
                return true;
            }
        }

        return false;
    }

    /**
     * One Split Per Region is used in the case where we are computing statistics on the table.
     *
     * @param configuration
     * @return
     */
    public static boolean oneSplitPerRegion( Configuration configuration) {
        String oneSplitPerRegion = configuration.get(MRConstants.ONE_SPLIT_PER_REGION);
        return oneSplitPerRegion != null && oneSplitPerRegion.compareToIgnoreCase("TRUE") == 0;
    }

    public static Double sampling( Configuration configuration) {
        String sampling = configuration.get(MRConstants.SPLICE_SAMPLING);
        return sampling != null ? Double.parseDouble(sampling) : null;
    }

    /**
     * Allows subclasses to get the {@link HTable}.
     */
    protected Table getHTable() {
        return this.table;
    }

    /**
     * Allows subclasses to set the {@link HTable}.
     *
     * @param table  The table to get the data from.
     */
    protected void setHTable(Table table) {
        if (table == null) throw new IllegalArgumentException("Unexpected null value for 'table'.");
        this.table = table;
        if (conf == null) throw new RuntimeException("Unexpected null value for 'conf'");
        conf.set(TableInputFormat.INPUT_TABLE, table.getName().getNameAsString());
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
