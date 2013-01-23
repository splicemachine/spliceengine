package com.ir.hbase.txn.coprocessor.region.test;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.ir.hbase.txn.coprocessor.region.TransactionState;
import com.ir.hbase.txn.coprocessor.region.TxnUtils;
import com.ir.hbase.txn.coprocessor.region.WriteAction;
import com.ir.hbase.txn.test.TestConstants;

public class TxnUtilsTest extends TestConstants {
	private static final Log LOG = LogFactory.getLog(TransactionState.class);

	public static List<WriteAction> actions;
	public static final String startKey = "bb";
	
	public void prepareWriteActionList() {
		actions = new LinkedList<WriteAction>();
		String keyStr = startKey; 
		for (int i = 0; i < 3; ++i) {
			for (int j = 0; j < 3; ++j) {
				byte[] key = keyStr.getBytes();
				if (LOG.isDebugEnabled())
					LOG.debug("Generate write action with key " + Bytes.toString(key) + " for test");
				Put put = new Put(key);
				Delete delete = new Delete(key);
				delete.deleteColumn(DEFAULT_FAMILY_BYTES, COL2);
				put.add(DEFAULT_FAMILY_BYTES, COL1, VAL1);
				actions.add(new WriteAction(put, new EmulatedHRegion(), txnID1));
				actions.add(new WriteAction(delete, new EmulatedHRegion(), txnID1));
				keyStr = keyStr.substring(0, keyStr.length() - 1) + (char)(keyStr.charAt(keyStr.length() - 1) + 1);
			}
			keyStr = (char)(keyStr.charAt(0) + 1) + startKey.substring(1, startKey.length());
		}
	}
	
	@Test
	public void testSplitInRange() throws Exception {
		prepareWriteActionList();
		List<WriteAction> right = new LinkedList<WriteAction>();
		TxnUtils.splitWriteActionList("cc".getBytes(), actions, right);
		Assert.assertEquals(8, actions.size());
		Assert.assertEquals(10, right.size());
	}
	
	@Test
	public void testSplitBeforeRange() throws Exception {
		prepareWriteActionList();
		List<WriteAction> right = new LinkedList<WriteAction>();
		TxnUtils.splitWriteActionList("bb".getBytes(), actions, right);
		Assert.assertEquals(0, actions.size());
		Assert.assertEquals(18, right.size());
	}
	
	@Test
	public void testSplitAfterRange() throws Exception {
		prepareWriteActionList();
		List<WriteAction> right = new LinkedList<WriteAction>();
		TxnUtils.splitWriteActionList("de".getBytes(), actions, right);
		Assert.assertEquals(18, actions.size());
		Assert.assertEquals(0, right.size());
	}
}

