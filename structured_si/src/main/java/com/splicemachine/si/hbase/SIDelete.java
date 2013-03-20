package com.splicemachine.si.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.log4j.Logger;

import com.splicemachine.impl.si.txn.Transaction;
import com.splicemachine.si.utils.SIConstants;

public class SIDelete extends Delete {
	private static Logger LOG = Logger.getLogger(SIDelete.class);
	public SIDelete(Transaction transaction) {
		super();
		setAttribute(SIConstants.SI, SIConstants.EMPTY_BYTE_ARRAY);
		this.setTimestamp(transaction.getStartTimestamp());
	}
	public SIDelete(byte[] row, Transaction transaction) {
		super(row);
		setAttribute(SIConstants.SI, SIConstants.EMPTY_BYTE_ARRAY);
		this.setTimestamp(transaction.getStartTimestamp());
	}

	public SIDelete(byte[] row, long timestamp, RowLock rowLock, Transaction transaction) {
		super(row,timestamp,rowLock);
		setAttribute(SIConstants.SI, SIConstants.EMPTY_BYTE_ARRAY);
		this.setTimestamp(transaction.getStartTimestamp());

	}

}
