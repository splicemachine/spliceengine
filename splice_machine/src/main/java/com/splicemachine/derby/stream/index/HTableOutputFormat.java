package com.splicemachine.derby.stream.index;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.SMRecordWriter;
import com.splicemachine.derby.stream.output.SpliceOutputCommitter;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by jyuan on 10/19/15.
 */
public class HTableOutputFormat extends OutputFormat<byte[],KVPair> implements Configurable {
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
            TableWriter tableWriter = TableWriterUtils.deserializeTableWriter(taskAttemptContext.getConfiguration());
            TxnView childTxn = outputCommitter.getChildTransaction(taskAttemptContext.getTaskAttemptID());
            if (childTxn == null)
                throw new IOException("child transaction lookup failed");
            tableWriter.setTxn(outputCommitter.getChildTransaction(taskAttemptContext.getTaskAttemptID()));
            return new HTableRecordWriter(tableWriter);
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
                TableWriter tableWriter = TableWriterUtils.deserializeTableWriter(taskAttemptContext.getConfiguration());
                outputCommitter = new SpliceOutputCommitter(tableWriter.getTxn(),tableWriter.getDestinationTable());
            }
            return outputCommitter;
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }
}
