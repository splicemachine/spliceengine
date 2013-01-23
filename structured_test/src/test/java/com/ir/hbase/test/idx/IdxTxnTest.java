package com.ir.hbase.test.idx;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import junit.framework.Assert;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import com.ir.constants.SchemaConstants;
import com.ir.constants.TxnConstants;
import com.ir.hbase.test.BaseTest;
import com.ir.hbase.txn.TransactionManager;
import com.ir.hbase.txn.TransactionState;
//Note: run each test separately
public class IdxTxnTest extends BaseTest {
	
	 private static TransactionManager tm;
	 private static HTable txnTable;
	    
	@Test
	public void testTransactionIndexPut() throws IOException, KeeperException, InterruptedException, ExecutionException {
		//Begin transaction
		TransactionState state1 = tm.beginTransaction();
		String txnID = state1.getTransactionID();
		//Put data into table with transaction id
		IdxTestUtils.putDataToTable(htable, txnID);
		//Read uncommitted data before commit, should get the data from cache
		//As for index table, there are two indexes, 10 rows for each
		//As for regular table, 10 rows totally
		Assert.assertEquals(20, IdxTestUtils.countRowNum(indexTable, txnID));
		Assert.assertEquals(10, IdxTestUtils.countRowNum(htable, txnID));
		//Read committed data before commit, should get nothing
		Assert.assertEquals(0, IdxTestUtils.countRowNum(indexTable, null));
		Assert.assertEquals(0, IdxTestUtils.countRowNum(htable, null));
		//Call commit on TransactionManager
		tm.tryCommit(state1);
		//Read committed data after commit, should get data from HBase
		Assert.assertEquals(20, IdxTestUtils.countRowNum(indexTable, null));
		Assert.assertEquals(10, IdxTestUtils.countRowNum(htable, null));
	}
	/**
	 * Test index delete with transaction function
	 */
	@Test
	public void testTransactionIndexDelete() throws IOException, KeeperException, InterruptedException, ExecutionException {
		TransactionState state1 = tm.beginTransaction();
		String txnID = state1.getTransactionID();
		IdxTestUtils.putDataToTable(htable, txnID);
		
		Delete delete1 = new Delete(Bytes.toBytes(row2));
		delete1.deleteColumns(FAMILY3, COL5);
		delete1.deleteColumns(FAMILY3, COL6);
		//set id to enable transaction
		delete1.setAttribute(TxnConstants.TRANSACTION_ID, Bytes.toBytes(txnID));
		htable.delete(delete1);
		
		Delete delete2 = new Delete(Bytes.toBytes(row3));
		delete2.deleteColumns(FAMILY2, COL3);
		delete2.deleteColumns(FAMILY2, COL4);
		//set id to enable transaction
		delete2.setAttribute(TxnConstants.TRANSACTION_ID, Bytes.toBytes(txnID));
		htable.delete(delete2);
		
		//Read uncommitted data before commit, should get data from cache and delete1, delete2 have been applied.
		Assert.assertFalse(IdxTestUtils.containsColumn(INDEX2, row2, FAMILY3, COL5, indexTable, txnID));
		Assert.assertFalse(IdxTestUtils.containsColumn(INDEX2, row2, FAMILY3, COL6, indexTable, txnID));
		Assert.assertFalse(IdxTestUtils.containsColumn(INDEX2, row3, FAMILY2, COL3, indexTable, txnID));
		Assert.assertFalse(IdxTestUtils.containsColumn(INDEX2, row3, FAMILY2, COL4, indexTable, txnID));
		//Read committed data before commit, should get nothing
		Assert.assertEquals(0, IdxTestUtils.countRowNum(indexTable, null));
		Assert.assertEquals(0, IdxTestUtils.countRowNum(htable, null));
		//Call commit
		tm.tryCommit(state1);
		//Read committed data after commit
		Assert.assertFalse(IdxTestUtils.containsColumn(INDEX2, "ROW2", FAMILY3, COL5, indexTable, null));
		Assert.assertFalse(IdxTestUtils.containsColumn(INDEX2, "ROW2", FAMILY3, COL6, indexTable, null));
		Assert.assertFalse(IdxTestUtils.containsColumn(INDEX2, "ROW3", FAMILY2, COL3, indexTable, null));
		Assert.assertFalse(IdxTestUtils.containsColumn(INDEX2, "ROW3", FAMILY2, COL4, indexTable, null));
		Assert.assertEquals(20, IdxTestUtils.countRowNum(indexTable, null));
		Assert.assertEquals(10, IdxTestUtils.countRowNum(htable, null));
	}
	
	@BeforeClass
	public static void setUp() throws Exception {
		basicConf();
		loadIdxStructuredTxnCoprocessor();
		start();
		//create table
		hbaseTestingUtility.getHBaseAdmin().createTable(IdxTestUtils.generateIdxStructuredTableDescriptor());
		htable = new HTable(hbaseTestingUtility.getConfiguration(), REGULAR_TABLE1);
		indexTable = new HTable(hbaseTestingUtility.getConfiguration(), Bytes.toBytes(regTable1 + SchemaConstants.INDEX));
	
		txnTable = hbaseTestingUtility.createTable(TxnConstants.TRANSACTION_TABLE_BYTES, TxnConstants.TRANSACTION_TABLE_BYTES);
		tm = new TransactionManager(hbaseTestingUtility.getConfiguration(), txnTable);
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		end();
	}
}