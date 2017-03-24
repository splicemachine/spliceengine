package com.splicemachine.orc.input;

import org.apache.hadoop.hive.ql.io.orc.SpliceOrcUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.joda.time.DateTimeZone;
import java.io.IOException;
import java.util.List;

/**
 * Created by jleach on 3/21/17.
 */
public class SpliceOrcNewInputFormat extends InputFormat<NullWritable,Row>
        implements DataSourceRegister {
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.UTC;

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        return SpliceOrcUtils.getSplits(jobContext);
    }

    @Override
    public RecordReader<NullWritable, Row> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        OrcMapreduceRecordReader reader = new OrcMapreduceRecordReader();
        reader.initialize(inputSplit,taskAttemptContext);
        return reader;
    }

    @Override
    public String shortName() {
        return "sorc";
    }



}