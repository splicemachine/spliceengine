package com.splicemachine.derby.stream.index;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by jyuan on 10/19/15.
 */
public class HTableRecordWriter extends RecordWriter<byte[],KVPair> {
    private static Logger LOG = Logger.getLogger(HTableRecordWriter.class);
    boolean initialized = false;
    TableWriter tableWriter;
    public HTableRecordWriter(TableWriter tableWriter) {
        SpliceLogUtils.trace(LOG, "init");
        this.tableWriter = tableWriter;
    }

    @Override
    public void write(byte[] rowKey, KVPair kvPair) throws IOException, InterruptedException {
        try {
            if (!initialized) {
                initialized = true;
                tableWriter.open();
            }
            tableWriter.write(kvPair);
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
