package com.splicemachine.compactions;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.mrio.MRConstants;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jyuan on 3/24/16.
 */
public class CompactionRecordReader extends RecordReader<Integer, Iterator> {

    private Integer currentKey;
    private Iterator<String> currentValue;
    private Configuration conf;
    private List<String> files;
    private boolean consumed;
    public CompactionRecordReader(Configuration conf) {
        this.conf = conf;
        this.consumed = false;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
        String fileString = conf.get(MRConstants.COMPACTION_FILES);
        files = SerializationUtils.deserialize(Base64.decodeBase64(fileString));
        currentKey = new Integer(0);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!consumed) {
            currentKey = new Integer(1);
            currentValue = files.iterator();
            consumed = true;

            return true;
        }
        else {
            currentKey = null;
            currentValue = null;

            return false;
        }
    }

    @Override
    public Integer getCurrentKey() throws IOException, InterruptedException {

        return currentKey;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public Iterator<String> getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

}
