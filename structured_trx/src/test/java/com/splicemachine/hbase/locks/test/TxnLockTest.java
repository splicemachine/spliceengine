package com.splicemachine.hbase.locks.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.splicemachine.hbase.txn.TransactionState;
import com.splicemachine.hbase.txn.test.BaseTest;

public class TxnLockTest extends BaseTest {
	static final Log LOG = LogFactory.getLog(TxnLockTest.class);
	private static HTable htable1;
	private String testTable1 = "Table";
	volatile boolean check = false;

	@Before
	public void createTable() throws Exception {
		if (admin.tableExists(testTable1)) {
			admin.disableTable(testTable1);
			admin.deleteTable(testTable1);
		}
		htable1 = hbaseTestingUtility.createTable(testTable1.getBytes(), DEFAULT_FAMILY.getBytes());
	}

	@Test
	public void testDirtyRead() throws Exception {
		//gfan: Dirty Read doesn't happen for current framework which doesn't allow one transaction state reading from another
	}
	
	@Test
	public void testNonrepeatableRead() throws Exception {
		LockTestUtils.putSingleCell(htable1, VAL1, null);
		TransactionState state1 = tm.beginTransaction(true, false, false, null);
		byte[] result1 = LockTestUtils.getSingleCell(htable1, state1.getTransactionID(), TransactionIsolationLevel.REPEATABLE_READ);
		check = false;
		(new Thread() {
			public void run() {
				try {
					TransactionState state2 = tm.beginTransaction(true, false, false, null);
					LockTestUtils.putSingleCell(htable1, VAL2, state2.getTransactionID());
					Assert.assertTrue(check);
				} catch (Exception e) {
					e.printStackTrace();
				}
			};
		}).start();
		byte[] result2 =  LockTestUtils.getSingleCell(htable1, state1.getTransactionID(), TransactionIsolationLevel.REPEATABLE_READ);
		Assert.assertTrue(Bytes.equals(result1, result2));
		tm.tryCommit(state1);
		check = true;
	}
}