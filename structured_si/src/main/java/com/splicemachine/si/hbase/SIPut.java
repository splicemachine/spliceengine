package com.splicemachine.si.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;

public class SIPut extends Put{
	public SIPut() {
		super();
	}
	public SIPut(byte[] row) {
		super(row);
	}

	public SIPut(Put put) {
		super(put);
	}

	public SIPut(byte[] row, long timestamp) {
		super(row,timestamp);
	}

	public SIPut(byte[] row, RowLock rowLock) {
		super(row,rowLock);
	}

	public SIPut(byte[] row,  long timestamp, RowLock rowLock) {
		super(row, timestamp, rowLock);
	}

}
