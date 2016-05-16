package com.splicemachine.stream.index;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import scala.util.Either;

import java.io.IOException;

/**
 * Created by jyuan on 10/19/15.
 */
public class HTableRecordWriter extends RecordWriter<byte[],Either<Exception, KVPair>> {
    private static Logger LOG = Logger.getLogger(HTableRecordWriter.class);
    boolean initialized = false;
    TableWriter tableWriter;
    OutputCommitter outputCommitter;
    private boolean failure = false;

    public HTableRecordWriter(TableWriter tableWriter, OutputCommitter outputCommitter) {
        SpliceLogUtils.trace(LOG, "init");
        this.tableWriter = tableWriter;
        this.outputCommitter = outputCommitter;
    }

    @Override
    public void write(byte[] rowKey, Either<Exception, KVPair> value) throws IOException, InterruptedException {
        if (value.isLeft()) {
            Exception e = value.left().get();
            // failure
            failure = true;
            SpliceLogUtils.error(LOG,"Error Reading",e);
            throw new IOException(e);
        }
        assert value.isRight();
        KVPair kvPair = value.right().get();
        try {
            if (!initialized) {
                initialized = true;
                tableWriter.open();
            }
            tableWriter.write(kvPair);
        } catch (Exception se) {
            SpliceLogUtils.error(LOG,"Error Writing",se);
            failure = true;
            throw new IOException(se);
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        SpliceLogUtils.trace(LOG,"closing %s",taskAttemptContext);
        try {
            if (initialized) {
                tableWriter.close();
            }
        } catch (Exception e) {
            failure = true;
            throw new IOException(e);
        } finally {
            if (failure)
                outputCommitter.abortTask(taskAttemptContext);
        }
    }
}
