package com.splicemachine.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;


public class SpliceTableInputFormat extends InputFormat<HBaseRowLocation,ExecRow> {

	@Override
	public RecordReader<HBaseRowLocation, ExecRow> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,InterruptedException {
		return null;
	}

	@Override
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException,InterruptedException {
		return null;
	}


}
