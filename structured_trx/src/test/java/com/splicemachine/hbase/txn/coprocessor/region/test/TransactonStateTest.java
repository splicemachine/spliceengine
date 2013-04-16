package com.splicemachine.hbase.txn.coprocessor.region.test;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.hbase.txn.TransactionStatus;
import com.splicemachine.hbase.txn.coprocessor.region.TransactionState;
import com.splicemachine.hbase.txn.coprocessor.region.TransactionState.SplitPointPosition;
import com.splicemachine.hbase.txn.test.ZkBaseTest;

public class TransactonStateTest extends ZkBaseTest {
	private static final Log LOG = LogFactory.getLog(TransactionState.class);
	
	public static final String txnLogPath = "/txnLogPath";
	public static TransactionState ts;
	public static String startKey = "bb"; 
	
	@BeforeClass
	public static void prepareTransactionState() throws Exception {
		ZKUtil.createWithParents(zkw, txnID1);
		rzk.setData(txnID1, TransactionStatus.PENDING.toString().getBytes(), -1);
		String keyStr = startKey; 
		ts = new TransactionState(txnID1, new EmulatedHRegion(), rzk, null, null, null, txnLogPath, false);
		for (int i = 0; i < 3; ++i) {
			for (int j = 0; j < 3; ++j) {
				byte[] key = keyStr.getBytes();
				if (LOG.isDebugEnabled())
					LOG.debug("Generate write action with key " + Bytes.toString(key) + " for test");
				Put put = new Put(key);
				Delete delete = new Delete(key);
				delete.deleteColumn(DEFAULT_FAMILY_BYTES, COL2);
				put.add(DEFAULT_FAMILY_BYTES, COL1, VAL1);
				ts.addDelete(delete);
				ts.addWrite(put);
				keyStr = keyStr.substring(0, keyStr.length() - 1) + (char)(keyStr.charAt(keyStr.length() - 1) + 1);
			}
			keyStr = (char)(keyStr.charAt(0) + 1) + startKey.substring(1, startKey.length());
		}
	}
	
	@Test
	public void testGetWriteActionRange() {
		byte[][] range = ts.getWriteActionRange();
		Assert.assertTrue(Bytes.equals("bb".getBytes(), range[0]));
		Assert.assertTrue(Bytes.equals("dd".getBytes(), range[1]));
	}
	
	@Test
	public void testBeforeStateRange() {
		Assert.assertEquals(SplitPointPosition.BEFORE_STATE_RANGE, ts.getSplitPointPosition("aa".getBytes()));
	}
	
	@Test
	public void testInStateRange() {
		Assert.assertEquals(SplitPointPosition.IN_STATE_RANGE, ts.getSplitPointPosition("bb".getBytes()));
		Assert.assertEquals(SplitPointPosition.IN_STATE_RANGE, ts.getSplitPointPosition("cc".getBytes()));
		Assert.assertEquals(SplitPointPosition.IN_STATE_RANGE, ts.getSplitPointPosition("dd".getBytes()));
	}
	
	@Test
	public void testAfterStateRange() {
		Assert.assertEquals(SplitPointPosition.AFTER_STATE_RANGE, ts.getSplitPointPosition("df".getBytes()));
	}	
}