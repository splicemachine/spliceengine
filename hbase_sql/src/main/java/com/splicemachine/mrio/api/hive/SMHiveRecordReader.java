/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.mrio.api.hive;

import java.io.IOException;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;
import com.splicemachine.mrio.api.core.SMRecordReaderImpl;
import com.splicemachine.mrio.api.serde.ExecRowWritable;
import com.splicemachine.mrio.api.serde.RowLocationWritable;
import com.splicemachine.utils.SpliceLogUtils;
/**
 * 
 * Wrapper Class to Support Hive MR1
 * 
 *
 */
public class SMHiveRecordReader implements RecordReader<RowLocationWritable,ExecRowWritable> {
    protected static final Logger LOG = Logger.getLogger(SMHiveRecordReader.class);
	protected SMRecordReaderImpl delegate;
	protected RowLocationWritable key; 
	protected ExecRowWritable value;
	
	public SMHiveRecordReader(SMRecordReaderImpl reader) {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "SMWrappedRecordReader with reader=%s",reader);
		this.delegate = reader;	
	}
	
	@Override
	public boolean next(RowLocationWritable key, ExecRowWritable value) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "next with delegate=%s, key=%s, value=%s", delegate, key, value);
		try {
			boolean returnValue = delegate.nextKeyValue();
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "delegate returned %s", delegate, key, value);			
			key.set(delegate.getCurrentKey());
			value.set(delegate.getCurrentValue());
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "next received key=%s, value=%s, returnValue=%s",key,value,returnValue);
			return returnValue;
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public RowLocationWritable createKey() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "createKey with delegate=%s",delegate);
		try {
			RowLocationWritable writable = new RowLocationWritable();
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "createKey with writable=%s",writable);			
			return writable;
		} catch (Exception e) {
			throw new RuntimeException(e); // Not Possible
		}
	}

	@Override
	public ExecRowWritable createValue() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "createValue");
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "createValue with delegate=%s, formatIds=%s",delegate, delegate.getExecRow());
		try {
			return new ExecRowWritable(delegate.getExecRow());
		} catch (Exception e) {
			throw new RuntimeException(e); // Not Possible
		}
	}

	@Override
	public long getPos() throws IOException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "close");
		delegate.close();		
	}

	@Override
	public float getProgress() throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getProgress");
		try {
			return delegate.getProgress();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

}
