package com.splicemachine.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Created by jleach on 5/18/15.
 */
public class SMOutputFormat extends OutputFormat<RowLocation,ExecRow> implements Configurable {
    private static Logger LOG = Logger.getLogger(SMOutputFormat.class);
    protected Configuration conf;
    protected SpliceOutputCommitter outputCommitter;
    protected TxnView parentTxn;
    public SMOutputFormat() {
        super();
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            DataSetWriter dsWriter = TableWriterUtils.deserializeTableWriter(taskAttemptContext.getConfiguration());
            TxnView childTxn = outputCommitter.getChildTransaction(taskAttemptContext.getTaskAttemptID());
            if (childTxn == null)
                throw new IOException("child transaction lookup failed");
            dsWriter.setTxn(outputCommitter.getChildTransaction(taskAttemptContext.getTaskAttemptID()));
            return new SMRecordWriter(dsWriter.getTableWriter());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"getOutputCommitter for taskAttemptContext=%s",taskAttemptContext);
        try {
            if (outputCommitter == null) {
                DataSetWriter tableWriter = TableWriterUtils.deserializeTableWriter(taskAttemptContext.getConfiguration());
                outputCommitter = new SpliceOutputCommitter(tableWriter.getTxn(),tableWriter.getDestinationTable());
            }
            return outputCommitter;
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }

}