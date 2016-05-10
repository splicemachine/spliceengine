package com.splicemachine.derby.stream.spark.fake;

import com.splicemachine.stream.output.SMOutputFormat;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FakeOutputFormat extends SMOutputFormat {
    public FakeOutputFormat() {
        super();
    }
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.outputCommitter = new FakeOutputCommitter();
        return outputCommitter;
    }
}