package com.splicemachine.mrio.api.core;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 *
 * This class exists to simply shuffle the splits
 *
 * Created by jleach on 3/17/16.
 */
public class SMTextInputFormat extends TextInputFormat {

    /*
    * Protects Splice Machine in the case of loading ordered data
    *
    * Shuffle is performed in linear time (i.e. slows as number of splits increases)
    *
    */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> inputSplits = super.getSplits(job);
        Collections.shuffle(inputSplits);
        return inputSplits;
    }
}
