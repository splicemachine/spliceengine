/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.mrio.api.core;

import com.google.common.collect.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.impl.HMissedSplitException;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.HScan;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;
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
import java.util.List;

/**
 * Created by dgomezferro on 11/30/15.
 */
public abstract class AbstractSMInputFormat<K,V> extends InputFormat<K, V> implements Configurable {
    protected static final Logger LOG = Logger.getLogger(AbstractSMInputFormat.class);
    private static int MAX_RETRIES = 30;
    protected Configuration conf;
    protected Table table;

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

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
            InterruptedException {
        setConf(context.getConfiguration());
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getSplits with context=%s",context);
        Scan s;
        try {
            TableScannerBuilder tsb = TableScannerBuilder.getTableScannerBuilderFromBase64String(conf.get(MRConstants.SPLICE_SCAN_INFO));
            s = ((HScan)tsb.getScan()).unwrapDelegate();
        } catch (StandardException e) {
            SpliceLogUtils.error(LOG, e);
            throw new IOException(e);
        }
        SIDriver driver = SIDriver.driver();
        HBaseConnectionFactory instance = HBaseConnectionFactory.getInstance(driver.getConfiguration());
        Clock clock = driver.getClock();
        Connection connection = instance.getConnection();
        Partition clientPartition = new ClientPartition(connection, table.getName(), table, clock, driver.getPartitionInfoCache());
        int retryCounter = 0;
        boolean refresh = false;
        while (true) {
            try {
                List<Partition> splits = clientPartition.subPartitions(s.getStartRow(), s.getStopRow(), refresh);

                if (oneSplitPerRegion(conf))
                    return toSMSplits(splits);
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "getSplits " + splits);
                    for (Partition split : splits) {
                        SpliceLogUtils.debug(LOG, "split -> " + split);
                    }
                }
                SubregionSplitter splitter = new HBaseSubregionSplitter();

                return splitter.getSubSplits(table, splits, s.getStartRow(), s.getStopRow());
            } catch (HMissedSplitException e) {
                // retry;
                refresh = true;
                LOG.warn("Missed split computing subtasks for region " + clientPartition);
                retryCounter++;
                if (retryCounter > MAX_RETRIES) {
                    throw e;
                }
            }
        }
    }

    /**
     * One Split Per Region is used in the case where we are computing statistics on the table.
     *
     * @param configuration
     * @return
     */
    public static boolean oneSplitPerRegion( Configuration configuration) {
        String oneSplitPerRegion = configuration.get(MRConstants.ONE_SPLIT_PER_REGION);
        if (oneSplitPerRegion != null && oneSplitPerRegion.compareToIgnoreCase("TRUE") == 0)
                return true;
        return false;
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
