package com.splicemachine.si.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.log4j.Logger;

import com.splicemachine.si.utils.SIConstants;

public class SIGet extends Get {
	private static Logger LOG = Logger.getLogger(SIGet.class);
	
	public SIGet() {
		super();
	}
	
	public SIGet(long timestamp) throws IOException {
		super();
		this.setTimeRange(0, timestamp);
		init();
	}

	public SIGet(byte[] row, long timestamp) throws IOException {
		super(row);
		this.setTimeRange(0, timestamp);
		init();
	}

	public SIGet(byte[] row, RowLock rowLock, long timestamp) throws IOException {
		super(row,rowLock);
		this.setTimeRange(0, timestamp);
		init();
	}
	public void init() {
		this.setAttribute(SIConstants.SI, SIConstants.EMPTY_BYTE_ARRAY);
	}
}
