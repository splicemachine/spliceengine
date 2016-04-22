package com.splicemachine.stream.output;

import com.splicemachine.db.iapi.error.ExceptionSeverity;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Created by jleach on 5/18/15.
 */
public class SMRecordWriter extends RecordWriter<RowLocation,Object> {
    private static Logger LOG = Logger.getLogger(SMRecordWriter.class);
    boolean initialized = false;
    TableWriter tableWriter;
    OutputCommitter outputCommitter;
    private boolean failure = false;

    public SMRecordWriter(TableWriter tableWriter, OutputCommitter outputCommitter) {
        SpliceLogUtils.trace(LOG,"init");
        this.tableWriter = tableWriter;
        this.outputCommitter = outputCommitter;
    }

    @Override
    public void write(RowLocation rowLocation, Object value) throws IOException, InterruptedException {
        if (value instanceof Exception) {
            Exception e = (Exception) value;
            // failure
            failure = true;
            SpliceLogUtils.error(LOG,"Error Reading",e);
            throw new IOException(e);
        }
        assert value instanceof ExecRow;
        ExecRow execRow = (ExecRow) value;
        try {
            if (!initialized) {
                initialized = true;
                tableWriter.open();
            }
            tableWriter.write(execRow);
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
