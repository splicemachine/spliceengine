/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.mrio.api.mapreduce;

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import com.splicemachine.mrio.api.core.SMRecordReaderImpl;
import com.splicemachine.mrio.api.serde.ExecRowWritable;
import com.splicemachine.mrio.api.serde.RowLocationWritable;
import com.splicemachine.utils.SpliceLogUtils;
/**
 * 
 * Wrapper Class to Support Hive MR2 while deferring to Splice Machine's core MR format.
 * 
 *
 */
public class SMWrappedRecordReader extends RecordReader<RowLocationWritable,ExecRowWritable> {
    protected static final Logger LOG = Logger.getLogger(SMWrappedRecordReader.class);
	protected SMRecordReaderImpl delegate;
	protected RowLocationWritable key; 
	protected ExecRowWritable value;
	
	public SMWrappedRecordReader(SMRecordReaderImpl reader) {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "SMWrappedRecordReader with reader=%s",reader);
		this.delegate = reader;	
		this.key = new RowLocationWritable();
		this.value = new ExecRowWritable(delegate.getExecRow());
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		delegate.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return delegate.nextKeyValue();
	}

	@Override
	public RowLocationWritable getCurrentKey() throws IOException, InterruptedException {
		key.set(delegate.getCurrentKey());
		return key;
	}

	@Override
	public ExecRowWritable getCurrentValue() throws IOException,
			InterruptedException {
		value.set(delegate.getCurrentValue());
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return delegate.getProgress();
	}

	@Override
	public void close() throws IOException {
		 delegate.close();
	}

}
