package com.splicemachine.si.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.log4j.Logger;

import com.splicemachine.impl.si.txn.Transaction;

public class SIDelete extends Delete {
	private static Logger LOG = Logger.getLogger(SIDelete.class);
	public SIDelete(Transaction transaction) {
		super();
	}
	public SIDelete(byte[] row, Transaction transaction) {
		super(row);
	}

	public SIDelete(byte[] row, long timestamp, RowLock rowLock, Transaction transaction) {
		super(row,timestamp,rowLock);
	}

}
