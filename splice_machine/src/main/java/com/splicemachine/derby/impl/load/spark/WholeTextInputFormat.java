package com.splicemachine.derby.impl.load.spark;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.spark.input.WholeTextFileRecordReader;

import java.io.IOException;

public class WholeTextInputFormat extends CombineFileInputFormat<String, String> implements Configurable {

    private Configuration conf;

    public WholeTextInputFormat() {
    }

    @Override
    public RecordReader<String, String> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        taskAttemptContext.setStatus(inputSplit.toString());

        WholeTextFileRecordReader reader = new WholeTextFileRecordReader((CombineFileSplit) inputSplit, taskAttemptContext, 0);
        reader.setConf(conf);
        return reader;
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}