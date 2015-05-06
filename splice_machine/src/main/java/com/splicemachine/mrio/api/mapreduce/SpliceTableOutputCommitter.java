package com.splicemachine.mrio.api.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SpliceTableOutputCommitter extends OutputCommitter {

	@Override
	public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
		// Rollback Txn...
	}

	@Override
	public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
		// Commit Txn...
	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void setupJob(JobContext jobContext) throws IOException {
		
	}

	@Override
	public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
		// Create Sub Transaction
	}

}
