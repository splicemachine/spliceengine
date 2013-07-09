package com.splicemachine.spark;

import java.io.IOException;
import java.util.List;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SpliceInputFormat extends InputFormat<byte[],ExecRow> {

	@Override
	public RecordReader<byte[], ExecRow> createRecordReader(InputSplit inputSplit,TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return null;
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		return null;
	}

}
