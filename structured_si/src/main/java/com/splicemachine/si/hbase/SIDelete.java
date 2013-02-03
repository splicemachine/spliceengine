package com.splicemachine.si.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.log4j.Logger;

public class SIDelete extends Put {
	private static Logger LOG = Logger.getLogger(SIDelete.class);
	public SIDelete() {
		super();
	}
	public SIDelete(byte[] row) {
		super(row);
	}

	public SIDelete(byte[] row, long timestamp, RowLock rowLock) {
		super(row,timestamp,rowLock);
	}

}
