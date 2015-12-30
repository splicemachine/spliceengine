package com.splicemachine.derby.stream.index;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.impl.job.scheduler.SubregionSplitter;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by jyuan on 10/19/15.
 */
public class HTableInputFormat extends InputFormat<byte[], KVPair> implements Configurable {
    protected static final Logger LOG = Logger.getLogger(HTableInputFormat.class);
    protected Configuration conf;
    protected Table table;
    protected Scan scan;
    protected SMSQLUtil util;
    protected HTableRecordReader rr;
    protected boolean spark;

    @Override
    public void setConf(Configuration conf) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "setConf conf=%s", conf);
        String tableName = conf.get(MRConstants.SPLICE_INPUT_TABLE_NAME);
        String conglomerate = conf.get(MRConstants.SPLICE_INPUT_CONGLOMERATE);
        String tableScannerAsString = conf.get(MRConstants.SPLICE_SCAN_INFO);
        spark = tableScannerAsString!=null;
        conf.setBoolean("splice.spark", spark);
        String jdbcString = conf.get(MRConstants.SPLICE_JDBC_STR);
        String rootDir = conf.get(HConstants.HBASE_DIR);
        if (!spark && util==null)
            util = SMSQLUtil.getInstance(jdbcString);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "setConf tableName=%s, conglomerate=%s, tableScannerAsString=%s"
                    + "jdbcString=%s, rootDir=%s",tableName,conglomerate,tableScannerAsString,jdbcString, rootDir);
        if (conglomerate ==null && !spark) {
            LOG.error("Conglomerate not provided when spark is activated");
            throw new RuntimeException("Conglomerate not provided when spark is activated");
        }
        if (tableName == null && conglomerate == null) {
            LOG.error("Table Name Supplied is null");
            throw new RuntimeException("Table Name Supplied is Null");
        }
        if (conglomerate == null) {
            if (jdbcString == null) {
                LOG.error("JDBC String Not Supplied");
                throw new RuntimeException("JDBC String Not Supplied");
            }
            try {
                conglomerate = util.getConglomID(tableName);
                conf.set(MRConstants.SPLICE_INPUT_CONGLOMERATE, conglomerate);
            } catch (SQLException e) {
                LOG.error(StringUtils.stringifyException(e));
                throw new RuntimeException(e);
            }
        }
        try {
            if (spark)
                setHTable(SpliceAccessManager.getHTable(conglomerate));
            else
                setHTable(new HTable(conf,conglomerate));
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }
        if (tableScannerAsString == null) {
            if (jdbcString == null) {
                LOG.error("JDBC String Not Supplied");
                throw new RuntimeException("JDBC String Not Supplied");
            }
            try {
                conf.set(MRConstants.SPLICE_SCAN_INFO, util.getTableScannerBuilder(tableName, null).getTableScannerBuilderBase64String());
            } catch (Exception e) {
                LOG.error(StringUtils.stringifyException(e));
                throw new RuntimeException(e);
            }
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "finishingSetConf");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
            InterruptedException {
        setConf(context.getConfiguration());
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getSplits with context=%s",context);
        TableInputFormat tableInputFormat = new TableInputFormat();
        conf.set(TableInputFormat.INPUT_TABLE,conf.get(MRConstants.SPLICE_INPUT_CONGLOMERATE));
        tableInputFormat.setConf(conf);
        try {
            tableInputFormat.setScan(HTableScannerBuilder.getTableScannerBuilderFromBase64String(conf.get(MRConstants.SPLICE_SCAN_INFO)).getScan());
        } catch (StandardException e) {
            SpliceLogUtils.error(LOG, e);
            throw new IOException(e);
        }
        List<InputSplit> splits = tableInputFormat.getSplits(context);
        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "getSplits " + splits);
            for (InputSplit split: splits) {
                SpliceLogUtils.debug(LOG, "split -> " + split);
            }
        }
        SubregionSplitter splitter = DerbyFactoryDriver.derbyFactory.getSubregionSplitter();
        List<InputSplit> results = splitter.getSubSplits(table, splits);
        return results;
    }

    public HTableRecordReader getRecordReader(InputSplit split, Configuration config) throws IOException,
            InterruptedException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getRecorderReader with table=%s, conglomerate=%s",table,config.get(MRConstants.SPLICE_INPUT_CONGLOMERATE));
        rr = new HTableRecordReader(config);
        if(table == null)
            table = new HTable(HBaseConfiguration.create(config), config.get(MRConstants.SPLICE_INPUT_CONGLOMERATE));
        rr.setHTable(table);
        //if (!conf.getBoolean("splice.spark", false))
        rr.init(config, split);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "returning record reader");
        return rr;
    }

    @Override
    public RecordReader<byte[], KVPair> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "createRecordReader for split=%s, context %s",split,context);
        if (rr != null)
            return rr;
        return getRecordReader(split,context.getConfiguration());
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
}
