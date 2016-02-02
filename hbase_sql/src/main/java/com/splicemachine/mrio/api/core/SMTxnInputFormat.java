package com.splicemachine.mrio.api.core;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * Input Format that requires the following items passed to it.
 *
 */
public class SMTxnInputFormat extends AbstractSMInputFormat<RowLocation, Transaction> {
    protected static final Logger LOG = Logger.getLogger(SMTxnInputFormat.class);
    protected Configuration conf;
    protected Table table;
    protected Scan scan;
    protected SMSQLUtil util;
    protected SMTxnRecordReaderImpl rr;

    @Override
    public void setConf(Configuration conf) {
        String tableName = HConfiguration.TRANSACTION_TABLE;
        String conglomerate = HConfiguration.TRANSACTION_TABLE;
        String tableScannerAsString = conf.get(MRConstants.SPLICE_SCAN_INFO);
        String jdbcString = conf.get(MRConstants.SPLICE_JDBC_STR);
        String rootDir = conf.get(HConstants.HBASE_DIR);
        if (util==null)
            util = SMSQLUtil.getInstance(jdbcString);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "setConf tableName=%s, conglomerate=%s, tableScannerAsString=%s"
                    + "jdbcString=%s, rootDir=%s",tableName,conglomerate,tableScannerAsString,jdbcString, rootDir);
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
            PartitionFactory tableFactory = SIDriver.driver().getTableFactory();
            setHTable(((ClientPartition)tableFactory.getTable(conglomerate)).unwrapDelegate());
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "finishingSetConf");
        this.conf = conf;
    }

    public SMTxnRecordReaderImpl getRecordReader(InputSplit split, Configuration config) throws IOException,
            InterruptedException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getRecorderReader with table=%s, conglomerate=%s",table,config.get(MRConstants.SPLICE_INPUT_CONGLOMERATE));
        rr = new SMTxnRecordReaderImpl(config);
        if(table == null)
            table = new HTable(HBaseConfiguration.create(config), config.get(MRConstants.SPLICE_INPUT_CONGLOMERATE));
        rr.setHTable(table);
        rr.init(config, split);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "returning record reader");
        return rr;
    }

    @Override
    public RecordReader<RowLocation, Transaction> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "createRecordReader for split=%s, context %s",split,context);
        if (rr != null)
            return rr;
        return getRecordReader(split,context.getConfiguration());
    }
}