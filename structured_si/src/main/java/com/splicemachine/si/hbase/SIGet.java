package com.splicemachine.si.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.log4j.Logger;

public class SIGet extends Get {
	private static Logger LOG = Logger.getLogger(SIGet.class);
	public SIGet(long timeStamp) throws IOException {
		super();
		this.setTimeRange(0, timeStamp);
	}

	public SIGet(byte[] row, long timestamp) {
		super(row);
	}

	public SIGet(byte[] row, RowLock rowLock, long timeStamp) {
		super(row,rowLock);
	}
}
