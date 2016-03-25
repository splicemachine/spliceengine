package com.splicemachine.compactions;

import com.google.common.collect.Lists;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.AbstractSMInputFormat;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jyuan on 3/24/16.
 */
public class CompactionInputFormat extends AbstractSMInputFormat<Integer, Iterator> {

    protected CompactionRecordReader recordReader;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        String regionLocation = conf.get(MRConstants.REGION_LOCATION);
        CompactionSplit split = new CompactionSplit(regionLocation);
        List<InputSplit> splits = Lists.newArrayList();
        splits.add(split);

        return splits;
    }

    @Override
    public RecordReader<Integer, Iterator> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {

        SpliceLogUtils.info(LOG, "createRecordReader for split=%s, context %s", split, context);
        if (recordReader != null)
            return recordReader;
        else {
            recordReader = new CompactionRecordReader(conf);
            return recordReader;
        }
    }
}
