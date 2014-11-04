package com.splicemachine.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class StoreFileRecordReader extends RecordReader<KeyValue, KeyValue> {
	private StoreFileScanner storeFileScanner;
	private KeyValue keyValue;
	public StoreFileRecordReader() {
		
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		keyValue = storeFileScanner.next();
		return keyValue!=null;
	}

	@Override
	public KeyValue getCurrentKey() throws IOException, InterruptedException {
		return keyValue;
	}

	@Override
	public KeyValue getCurrentValue() throws IOException, InterruptedException {
		return keyValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		storeFileScanner.close();
	}
	
	
}