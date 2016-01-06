package com.splicemachine.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Created by jleach on 5/18/15.
 */
public class SMRecordWriter extends RecordWriter<RowLocation,ExecRow> {
    private static Logger LOG = Logger.getLogger(SMRecordWriter.class);
    boolean initialized = false;
    TableWriter tableWriter;
    public SMRecordWriter(TableWriter tableWriter) {
        SpliceLogUtils.trace(LOG,"init");
        this.tableWriter = tableWriter;
    }

    @Override
    public void write(RowLocation rowLocation, ExecRow execRow) throws IOException, InterruptedException {
        try {
            if (!initialized) {
                initialized = true;
                tableWriter.open();
            }
            tableWriter.write(execRow);
        } catch (StandardException se) {
            SpliceLogUtils.error(LOG,"Error Writing",se);
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
            throw new IOException(e);
        }
    }
}
