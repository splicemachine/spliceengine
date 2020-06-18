package com.splicemachine.derby.stream.spark;

import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.task.JobContextImpl;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author Igor Praznik
 *
 * Callable for concurrent generation of subsplits..
 */

public class FetchSplitsJob implements Callable<List<InputSplit>> {
    public static final Map<String, Future<List<InputSplit>>> splitCache = new ConcurrentHashMap<>();

    private Configuration conf;

    public FetchSplitsJob(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public List<InputSplit> call() {
        SMInputFormat inputFormat = new SMInputFormat();
        try {
            Configuration confTemp = new Configuration(conf);
            confTemp.unset(MRConstants.SPLICE_SCAN_INPUT_SPLITS_ID);
            return inputFormat.getSplits(new JobContextImpl(confTemp, null));
        } catch (IOException | InterruptedException ie) {
            throw new RuntimeException(ie.getMessage(), ie);
        }

    }
}
