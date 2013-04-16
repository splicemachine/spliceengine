package com.splicemachine.hbase.locks.test;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.splicemachine.hbase.txn.TxnConstants;

public class LockTestUtils extends BaseLockTest {
	public static void putSingleCell(HTable table, byte[] value, String transactionID) throws Exception {
		Put put = new Put("ROW1".getBytes());
		if (transactionID != null)
			put.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
		put.add(DEFAULT_FAMILY_BYTES, COL1, value);
		table.put(put);
	}
	
	public static byte[] getSingleCell(HTable table, String transactionID, TransactionIsolationLevel isoLevel) throws Exception {
		Get get = new Get("ROW1".getBytes());
		get.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
		get.setAttribute(TRANSACTION_ISOLATION_LEVEL, isoLevel.toString().getBytes());
		get.addColumn(DEFAULT_FAMILY_BYTES, COL1);
		return table.get(get).value();
	}
}
