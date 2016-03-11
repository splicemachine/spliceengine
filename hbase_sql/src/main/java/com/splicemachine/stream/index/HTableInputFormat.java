package com.splicemachine.stream.index;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.IndexScanSetBuilder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.HBaseSubregionSplitter;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.mrio.api.core.SubregionSplitter;
import com.splicemachine.storage.HScan;
import com.splicemachine.utils.SpliceLogUtils;

/**
 *
 * Created by jyuan on 10/19/15.
 */
public class HTableInputFormat extends InputFormat<byte[], KVPair> implements Configurable{
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
//            if (spark)
//                setHTable(SpliceAccessManager.getHTable(conglomerate));
//            else
            TableName tableInfo=HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration()).getTableInfo(conglomerate);
            setHTable(new HTable(conf,tableInfo));
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }
        if (tableScannerAsString == null) {
            if (jdbcString == null) {
                LOG.error("JDBC String Not Supplied");
                throw new RuntimeException("JDBC String Not Supplied");
            }
            try {
                conf.set(MRConstants.SPLICE_SCAN_INFO, util.getTableScannerBuilder(tableName, null).base64Encode());
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
        String conglomerate = conf.get(MRConstants.SPLICE_INPUT_CONGLOMERATE);
        TableName hTableName = HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration()).getTableInfo(conglomerate);
        conf.set(TableInputFormat.INPUT_TABLE, hTableName.getNameAsString());
        tableInputFormat.setConf(conf);
        try {
            IndexScanSetBuilder isb=HTableScannerBuilder.getTableScannerBuilderFromBase64String(conf.get(MRConstants.SPLICE_SCAN_INFO));
            Scan s = ((HScan)isb.getScan()).unwrapDelegate();
            tableInputFormat.setScan(s);
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
        SubregionSplitter splitter = new HBaseSubregionSplitter();
        return splitter.getSubSplits(table, splits);
    }

    public HTableRecordReader getRecordReader(InputSplit split, Configuration config) throws IOException,
            InterruptedException {
        TableName tableName=HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration()).getTableInfo(config.get(MRConstants.SPLICE_INPUT_CONGLOMERATE));
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getRecorderReader with table=%s, conglomerate=%s",table,tableName);
        rr = new HTableRecordReader(config);
        if(table == null){
            table=new HTable(HBaseConfiguration.create(config),tableName);
        }
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
