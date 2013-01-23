package com.ir.hbase.server.test;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.ir.constants.TransactionStatus;
import com.ir.constants.TxnConstants;
import com.ir.hbase.txn.TransactionState;
import com.ir.hbase.txn.test.TxnTestUtils;

public class TransactionManagerTest2 extends BaseTestOnServer {
    static final Log LOG = LogFactory.getLog(TransactionManagerTest2.class);
    private static HTable htable1;
    @Test
	public void testBeginTransaction() throws Exception {
		TransactionState state1 = tm.beginTransaction();
		Assert.assertTrue(state1.getTransactionID().startsWith("/TRANSACTION_PATH/txn-"));
		Assert.assertTrue(state1.getTransactionID().endsWith("0000000000"));
	}
    
    @Test
    public void tesTransactionalPut() throws Exception {
		TransactionState state1 = tm.beginTransaction();
		TxnTestUtils.putSingleFamilyData(htable1, state1.getTransactionID());
		Assert.assertEquals(0, TxnTestUtils.countRow(htable1, null, null));
		tm.tryCommit(state1);
		Assert.assertEquals(1000, TxnTestUtils.countRow(htable1, null, null));
    }
    @Test
    public void testGetUnCommitted() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		TxnTestUtils.putSingleFamilyData(htable1, transactionID);
		for (int i=1; i<1001; ++i) {
			Get get = new Get(("ROW"+i).getBytes());
	    	get.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
	    	get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
	    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
			Assert.assertEquals("VAL1", Bytes.toString(htable1.get(get).value()));
		}
    }
    
    @Test
    public void testGetCommitted() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		TxnTestUtils.putSingleFamilyData(htable1, transactionID);
		for (int i=1; i<1001; ++i) {
			Get get = new Get(("ROW"+i).getBytes());
			Assert.assertEquals(null, Bytes.toString(htable1.get(get).value()));
		}
    }
    
    @Test
    public void testScanUnCommitted() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		TxnTestUtils.putSingleFamilyData(htable1, transactionID);
		
		Scan scan = new Scan(("ROW"+1).getBytes(), ("ROW"+1000).getBytes());
		scan.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
    	scan.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
		ResultScanner scanner = htable1.getScanner(scan);
		Result result = null;
		while ((result = scanner.next()) != null) {
			for (byte[] family : result.getMap().keySet())
				for (byte[] col : result.getFamilyMap(family).keySet())
					Assert.assertTrue(null, Bytes.toString(result.getFamilyMap(family).get(col)).startsWith("VAL"));
		}
    }
    
    @Test
    public void testScanCommitted() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		TxnTestUtils.putSingleFamilyData(htable1, transactionID);
		
		Scan scan = new Scan(("ROW"+1).getBytes(), ("ROW"+1000).getBytes());
		ResultScanner scanner = htable1.getScanner(scan);
		Assert.assertFalse(scanner.next() != null);
		scanner.close();
    }
    
    @Test
    public void testDeleteInMemory() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		TxnTestUtils.putSingleFamilyData(htable1, transactionID);
		for (int i=1; i<1001; ++i) {
			Delete delete = new Delete(("ROW"+i).getBytes());
	    	delete.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
	    	htable1.delete(delete);
		}
		Scan scan = new Scan(("ROW"+1).getBytes(), ("ROW"+1000).getBytes());
		scan.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
		ResultScanner scanner = htable1.getScanner(scan);
		Assert.assertFalse(scanner.next() != null);
		scanner.close();
    }
    
    @Test
    public void testPartialDeleteInMemory() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		TxnTestUtils.putSingleFamilyData(htable1, transactionID);
		for (int i=1; i<1001; ++i) {
			Delete delete = new Delete(("ROW"+i).getBytes());
			delete.deleteColumn(DEFAULT_FAMILY_BYTES, COL1);
	    	delete.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
	    	htable1.delete(delete);
		}
		for (int i=1; i<1001; ++i) {
			Get get = new Get(("ROW"+i).getBytes());
	    	get.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
	    	Result result = htable1.get(get);
			Assert.assertEquals(null, result.getColumnLatest(DEFAULT_FAMILY_BYTES, COL1));
			Assert.assertTrue(Bytes.equals(VAL2, result.getColumnLatest(DEFAULT_FAMILY_BYTES, COL2).getValue()));
		}
    }

	@Test
	public void testAbortTransaction() throws Exception {
		//need to setup abort conditions based on actual situation
	}
	/**
	 * Before fire prepare commit, transaction id node and all its children have PENDING;
	 * After fire prepare commit, they all have PREPARE_COMMIT.
	 * Their watching each other works.
	 * Two regions are visited in practice in this case.
	 */
	@Test
	public void testPrepareCommit() throws Exception {
		TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		TxnTestUtils.putSingleFamilyData(htable1, transactionID);
		Assert.assertEquals(TransactionStatus.PENDING, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID, false, null))));
		for(String node : rzk.getChildren(transactionID, false)) {
			Assert.assertEquals(TransactionStatus.PENDING, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID + "/" + node, false, null))));
		}
		tm.prepareCommit(state1);
		Assert.assertEquals(TransactionStatus.PREPARE_COMMIT, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID, false, null))));
		for(String node : rzk.getChildren(transactionID, false)) {
			Assert.assertEquals(TransactionStatus.PREPARE_COMMIT, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID + "/" + node, false, null))));
		}
	}

	@Test
	public void testDoCommit() throws Exception {
		//docommit is used in testTryCommit
	}
	/**
	 * Two regions are visited in practice in this case.
	 */
	@Test
	public void testTryCommit() throws Exception {
		TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		TxnTestUtils.putSingleFamilyData(htable1, transactionID);
		//Check status
		Assert.assertEquals(TransactionStatus.PENDING, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID, false, null))));
		for(String node : rzk.getChildren(transactionID, false)) {
			Assert.assertEquals(TransactionStatus.PENDING, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID + "/" + node, false, null))));
		}
		for (int i=1; i<1001; ++i) {
			Get get = new Get(("ROW"+i).getBytes());
			Assert.assertEquals(null, htable1.get(get).value());
		}
		//Try Commit
		tm.tryCommit(state1);
		//Check status
		for (int i=1; i<1001; ++i) {
			Get get = new Get(("ROW"+i).getBytes());
			Assert.assertTrue(Bytes.equals(VAL1, htable1.get(get).value()));
		}
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

