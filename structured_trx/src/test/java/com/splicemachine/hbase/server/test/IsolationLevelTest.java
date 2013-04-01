package com.splicemachine.hbase.server.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.splicemachine.hbase.txn.TransactionState;
import com.splicemachine.hbase.locks.test.LockTestUtils;

public class IsolationLevelTest extends BaseTestOnServer {

	static final Log LOG = LogFactory.getLog(TransactionManagerTest2.class);
	private static HTable htable1;
	volatile boolean check = false;

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

	@Before
	public void createTable() throws Exception {
		if (admin.tableExists(testTable1)) {
			admin.disableTable(testTable1);
			admin.deleteTable(testTable1);
		}
		HTableDescriptor desc = new HTableDescriptor(testTable1.getBytes());
		desc.addFamily(new HColumnDescriptor(DEFAULT_FAMILY.getBytes()));
		admin.createTable(desc);
		htable1 = new HTable(testTable1.getBytes());
	}
}
