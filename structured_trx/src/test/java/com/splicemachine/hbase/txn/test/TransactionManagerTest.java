package com.splicemachine.hbase.txn.test;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.splicemachine.hbase.txn.TransactionState;

public class TransactionManagerTest extends BaseTest {
	
	public HTable table;
  
    @Test
    public void testEmptyTransaction() throws Exception {
    	TransactionState state1 = tm.beginTransaction(true, false, false, null);
    	Assert.assertNotNull(state1.getTransactionID());
    	tm.prepareCommit(state1);
    	tm.doCommit(state1);
    }

    @Test
    public void validateRegularWrites() throws Exception {
    	TxnTestUtils.putDataToTable(table, 1,1000, null);
    	Assert.assertEquals(1000, hbaseTestingUtility.countRows(table));
    	table.close();
    }
    
    @Test
    public void testLargeCommit() throws Exception {
    	TransactionState state1 = tm.beginTransaction(true, false, false, null);
    	TxnTestUtils.putDataToTable(table, 101,100, state1.getTransactionID());
    	tm.prepareCommit(state1);
    	Assert.assertEquals(0, TxnTestUtils.countRow(table, 101, null));
    	Assert.assertEquals(100, TxnTestUtils.countRow(new HTable(hbaseTestingUtility.getConfiguration(), TRANSACTION_LOG_TABLE), null, null));
    	table.close();
    }

    @Test
    public void testManySmallCommits() throws Exception {
    	for (int i =300;i< 400;i++) {
        	TransactionState state1 = tm.beginTransaction(true, false, false, null);
        	TxnTestUtils.putDataToTable(table, i,1, state1.getTransactionID());
        	tm.prepareCommit(state1);
        	tm.doCommit(state1);
    	}
    	Assert.assertEquals(100, TxnTestUtils.countRow(table, 300, null));
    	table.flushCommits();
    	table.close();
    }

    
    @Test
    public void testAbort() throws Exception {
    	TransactionState state1 = tm.beginTransaction(true, false, false, null);
    	TxnTestUtils.putDataToTable(table,500, 100, state1.getTransactionID());
    	Assert.assertEquals(100, TxnTestUtils.countRow(table, 500,state1.getTransactionID()));
    	tm.abort(state1);
    	Assert.assertFalse(rzk.exists(state1.getTransactionID(), false) != null);
    	Assert.assertEquals(0, TxnTestUtils.countRow(table, 500, null));
    }
    
    @Before
	public void createTable() throws Exception {
		if (admin.tableExists(testTable1)) {
			admin.disableTable(testTable1);
			admin.deleteTable(testTable1);
		}
		HTableDescriptor desc = new HTableDescriptor(testTable1);
		desc.addFamily(new HColumnDescriptor(DEFAULT_FAMILY));
		admin.createTable(desc);
		table = new HTable(hbaseTestingUtility.getConfiguration(), testTable1.getBytes());
	}
}