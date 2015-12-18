package com.splicemachine.mrio.api.core;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.constants.SpliceConfiguration;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.impl.job.scheduler.SubregionSplitter;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * Input Format that requires the following items passed to it.
 *
 */
public class SMTxnInputFormat extends AbstractSMInputFormat<RowLocation, TxnMessage.Txn> {
    protected static final Logger LOG = Logger.getLogger(SMTxnInputFormat.class);
    protected Table table;
    protected Scan scan;
    protected SMTxnRecordReaderImpl rr;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        String tableName = SpliceConstants.TRANSACTION_TABLE;
        String conglomerate = SpliceConstants.TRANSACTION_TABLE;
        String rootDir = conf.get(HConstants.HBASE_DIR);
        conf.set(MRConstants.ONE_SPLIT_PER_REGION, "true");
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "setConf tableName=%s, conglomerate=%s, "
                    + "jdbcString=%s, rootDir=%s",tableName,conglomerate, rootDir);
        try {
            setHTable(SpliceAccessManager.getHTable(conglomerate));
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "finishingSetConf");
    }

    public SMTxnRecordReaderImpl getRecordReader(InputSplit split, Configuration config) throws IOException,
            InterruptedException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getRecorderReader with table=%s, conglomerate=%s",table,config.get(MRConstants.SPLICE_INPUT_CONGLOMERATE));
        rr = new SMTxnRecordReaderImpl(config);
        if(table == null)
            table = new HTable(HBaseConfiguration.create(config), config.get(TableInputFormat.INPUT_TABLE));
        rr.setHTable(table);
        rr.init(config, split);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "returning record reader");
        return rr;
    }

    @Override
    public RecordReader<RowLocation, TxnMessage.Txn> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "createRecordReader for split=%s, context %s",split,context);
        if (rr != null)
            return rr;
        return getRecordReader(split,context.getConfiguration());
    }
}