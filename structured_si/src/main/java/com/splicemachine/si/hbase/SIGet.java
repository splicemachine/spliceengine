package com.splicemachine.si.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RowLock;

public class SIGet extends Get {
	public SIGet() {
		super();
	}

	public SIGet(byte[] row) {
		super(row);
	}

	public SIGet(byte[] row, RowLock rowLock) {
		super(row,rowLock);
	}

	
}
