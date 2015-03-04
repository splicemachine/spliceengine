package com.splicemachine.mrio.api;

import java.io.IOException;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
/**
 * 
 * Record Writer for writing into Splice Machine. 
 * 
 *
 */
public class SMRecordWriter extends org.apache.hadoop.mapreduce.RecordWriter<ImmutableBytesWritable,ExecRow> {

	@Override
	public void close(TaskAttemptContext taskAttemptContext) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(ImmutableBytesWritable immutableBytesWritable, ExecRow execRow)
			throws IOException, InterruptedException {
		
	}

}
