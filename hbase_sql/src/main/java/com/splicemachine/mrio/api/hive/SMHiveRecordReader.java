/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
