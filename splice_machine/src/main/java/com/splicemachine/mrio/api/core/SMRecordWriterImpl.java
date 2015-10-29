package com.splicemachine.mrio.api.core;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;

public class SMRecordWriterImpl extends RecordWriter<RowLocation, ExecRow> {

	public SMRecordWriterImpl(Configuration conf) throws IOException {

	}
	@Override
	public void write(RowLocation ignore, ExecRow value) throws IOException,
			InterruptedException {
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {

	}
}
