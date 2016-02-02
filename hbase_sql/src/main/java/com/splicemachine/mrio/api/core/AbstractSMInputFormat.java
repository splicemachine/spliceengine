package com.splicemachine.mrio.api.core;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.storage.HScan;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
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
    protected Configuration conf;
    protected Table table;

    private List<InputSplit> toSMSplits (List<InputSplit> splits) throws IOException {
        List<InputSplit> sMSplits = Lists.newArrayList();
        for(InputSplit split:splits) {
            final TableSplit tableSplit = (TableSplit) split;
            SMSplit smSplit= new SMSplit(new TableSplit(tableSplit.getTable(), tableSplit.getStartRow(), tableSplit.getEndRow(), tableSplit.getRegionLocation()));
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
        TableInputFormat tableInputFormat = new TableInputFormat();
        conf.set(TableInputFormat.INPUT_TABLE,"splice:"+conf.get(MRConstants.SPLICE_INPUT_CONGLOMERATE));
        tableInputFormat.setConf(conf);
        try {
            TableScannerBuilder tsb = TableScannerBuilder.getTableScannerBuilderFromBase64String(conf.get(MRConstants.SPLICE_SCAN_INFO));
            Scan s = ((HScan)tsb.getScan()).unwrapDelegate();
            tableInputFormat.setScan(s);
        } catch (com.splicemachine.db.iapi.error.StandardException e) {
            SpliceLogUtils.error(LOG, e);
            throw new IOException(e);
        }
        List<InputSplit> splits = tableInputFormat.getSplits(context);
        String oneSplitPerRegion = conf.get(MRConstants.ONE_SPLIT_PER_REGION);
        if (oneSplitPerRegion == null || oneSplitPerRegion.compareToIgnoreCase("TRUE") == 0) {
            return toSMSplits(splits);
        }
        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "getSplits " + splits);
            for (InputSplit split: splits) {
                SpliceLogUtils.debug(LOG, "split -> " + split);
            }
        }
        SubregionSplitter splitter = new HBaseSubregionSplitter();
        List<InputSplit> results = splitter.getSubSplits(table, splits);
        return results;
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
        this.table = table;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
