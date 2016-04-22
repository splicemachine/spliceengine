package com.splicemachine.stream.index;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.stream.output.SpliceOutputCommitter;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by jyuan on 10/19/15.
 */
public class HTableOutputFormat extends OutputFormat<byte[],Object> implements Configurable {
    private static Logger LOG = Logger.getLogger(HTableOutputFormat.class);
    protected Configuration conf;
    protected SpliceOutputCommitter outputCommitter;
    protected TxnView parentTxn;

    public HTableOutputFormat() {
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
            DataSetWriterBuilder tableWriter =TableWriterUtils.deserializeTableWriter(taskAttemptContext.getConfiguration());
            TxnView childTxn = outputCommitter.getChildTransaction(taskAttemptContext.getTaskAttemptID());
            if (childTxn == null)
                throw new IOException("child transaction lookup failed");
            tableWriter.txn(childTxn);
            return new HTableRecordWriter(tableWriter.buildTableWriter(), outputCommitter);
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
            SpliceLogUtils.debug(LOG, "getOutputCommitter for taskAttemptContext=%s", taskAttemptContext);
        try {
            if (outputCommitter == null) {
                DataSetWriterBuilder tableWriter =TableWriterUtils.deserializeTableWriter(taskAttemptContext.getConfiguration());
                outputCommitter = new SpliceOutputCommitter(tableWriter.getTxn(),tableWriter.getDestinationTable());
            }
            return outputCommitter;
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }
}
