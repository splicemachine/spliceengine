package com.splicemachine.stream.index;

import java.io.IOException;
import java.sql.SQLException;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.mrio.api.core.AbstractSMInputFormat;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.ClientPartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.utils.SpliceLogUtils;

/**
 *
 * Created by jyuan on 10/19/15.
 */
public class HTableInputFormat extends AbstractSMInputFormat<byte[], KVPair> {
    protected static final Logger LOG = Logger.getLogger(HTableInputFormat.class);
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
            if (SIDriver.driver() == null) SpliceSpark.setupSpliceStaticComponents();
            PartitionFactory tableFactory=SIDriver.driver().getTableFactory();
            setHTable(((ClientPartition)tableFactory.getTable(conglomerate)).unwrapDelegate());
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

    public HTableRecordReader getRecordReader(InputSplit split, Configuration config) throws IOException,
            InterruptedException {
        TableName tableName=HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration()).getTableInfo(config.get(MRConstants.SPLICE_INPUT_CONGLOMERATE));
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getRecorderReader with table=%s, conglomerate=%s",table,tableName);
        rr = new HTableRecordReader(config);
        if(table == null){
            PartitionFactory tableFactory=SIDriver.driver().getTableFactory();
            table = ((ClientPartition)tableFactory.getTable(tableName)).unwrapDelegate();
        }
        rr.setHTable(table);
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

}
