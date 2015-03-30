package com.splicemachine.mrio.api.hive;

import java.io.IOException;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import com.splicemachine.mrio.api.core.SMRecordWriterImpl;
import com.splicemachine.mrio.api.serde.ExecRowWritable;
import com.splicemachine.mrio.api.serde.RowLocationWritable;

public class SMHiveRecordWriter implements RecordWriter<RowLocationWritable, ExecRowWritable> {
	protected SMRecordWriterImpl recordWriter;
	
	public SMHiveRecordWriter (SMRecordWriterImpl recordWriter) {
		this.recordWriter = recordWriter;
	}
	
	@Override
	public void write(RowLocationWritable key, ExecRowWritable value)
			throws IOException {
		try {
			recordWriter.write(key.get(), value.get());
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close(Reporter reporter) throws IOException {
		return; // ?
	}

}
