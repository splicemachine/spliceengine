package com.splicemachine.spark;

import java.io.IOException;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.splicemachine.derby.iapi.storage.RowProvider;

public class SpliceRecordReader extends RecordReader<RowLocation,ExecRow> {
	protected RowProvider rowProvider;
	public SpliceRecordReader(RowProvider rowProvider) {
		this.rowProvider = rowProvider;
	}
	
	
	@Override
	public void close() throws IOException {
		rowProvider.close();
	}

	@Override
	public RowLocation getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ExecRow getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

}
