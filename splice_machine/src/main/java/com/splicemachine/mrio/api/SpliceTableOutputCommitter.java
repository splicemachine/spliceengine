package com.splicemachine.mrio.api;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SpliceTableOutputCommitter extends OutputCommitter {

	@Override
	public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
		
	}

	@Override
	public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
		
	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setupJob(JobContext jobContext) throws IOException {
		
	}

	@Override
	public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
		
	}

}
