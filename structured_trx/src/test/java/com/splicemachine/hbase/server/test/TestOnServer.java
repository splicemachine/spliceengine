package com.splicemachine.hbase.server.test;

import org.apache.hadoop.hbase.client.HTable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.hbase.txn.TransactionState;
import com.splicemachine.hbase.txn.test.TxnTestUtils;

public class TestOnServer extends BaseTestOnServer {
	@Ignore
    @Test
    public void testEmptyTransaction() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
    	Assert.assertNotNull(state1.getTransactionID());
    	tm.prepareCommit(state1);
    	tm.doCommit(state1);
    }
	@Ignore
    @Test
    public void validateRegularWrites() throws Exception {
    	HTable table = getTestTable(admin, testTable1);
    	StringBuilder sb = new StringBuilder();
    	for (int i = 1; i < 11; ++i) {
	    	long start = System.currentTimeMillis();    
	    	TxnTestUtils.putDataToTable(table, 1,1000*i, null);
	    	long elapsedTime = System.currentTimeMillis() - start;
	    	sb.append("Running time for " + i*1000 + " rows: " + elapsedTime + "\n");
    	}
    	System.out.println(sb.toString());
    	table.close();
    }
	@Ignore
    @Test
    public void validateTransactionalWrites() throws Exception {
    	HTable table = getTestTable(admin, testTable1);
    	StringBuilder sb = new StringBuilder();
    	for (int i = 1; i < 6; ++i) {
	    	long start = System.currentTimeMillis();
	    	TransactionState state1 = tm.beginTransaction();
	    	TxnTestUtils.putDataToTable(table, 1,1000*i, state1.getTransactionID());
	    	tm.tryCommit(state1);
	    	long elapsedTime = System.currentTimeMillis() - start;
	    	sb.append("Running time for " + i*1000 + " rows: " + elapsedTime + "\n");
    	}
    	System.out.println(sb.toString());
    	table.close();
    }
	@Ignore
    @Test
    public void validateSmallTransactionalWrites() throws Exception {
    	HTable table = getTestTable(admin, testTable1);
    	StringBuilder sb = new StringBuilder();
    	long start1 = System.currentTimeMillis();
    	for (int j=0; j < 200; ++j) {
	    	long start = System.currentTimeMillis();
		    TransactionState state1 = tm.beginTransaction();
		    long elapsedTime1 = System.currentTimeMillis() - start;
	    	start = System.currentTimeMillis();
	    	for (int i = 1; i < 3; ++i) {
	    		TxnTestUtils.putDataToTable(table, i,1, state1.getTransactionID());
	    	}
		    long elapsedTime2 = System.currentTimeMillis() - start;
		    start = System.currentTimeMillis();
		    tm.prepareCommit(state1);
		    long elapsedTime3 = System.currentTimeMillis() - start;
	    	start = System.currentTimeMillis();
		    tm.doCommit(state1);
		    long elapsedTime4 = System.currentTimeMillis() - start;
		    sb.append("Begin transaction: " + elapsedTime1 + "\nPuts: " + elapsedTime2 + "\nPrepare Commit: " + elapsedTime3 + "\nDo Commit: " + elapsedTime4 + "\n\n");
    	}
	    long elapsedTime5 = System.currentTimeMillis() - start1;
	    sb.append("Total running time: " + elapsedTime5 + "\n");
    	System.out.println(sb.toString());
    	table.close();
    }
	@Ignore
    @Test
    public void testLargeCommit() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
    	HTable table = TxnTestUtils.getTestTable(admin, testTable1);
    	TxnTestUtils.putDataToTable(table, 101,100, state1.getTransactionID());
    	tm.prepareCommit(state1);
    	Assert.assertEquals(0, TxnTestUtils.countRow(table, 101, null));
    	Assert.assertEquals(100, TxnTestUtils.countRow(new HTable(config, TxnConstants.TRANSACTION_LOG_TABLE), null, null));
    	table.close();
    }
	@Ignore
    @Test
    public void testManySmallCommits() throws Exception {
    	HTable table = TxnTestUtils.getTestTable(admin, testTable1);
    	for (int i =300;i< 400;i++) {
        	TransactionState state1 = tm.beginTransaction();
        	TxnTestUtils.putDataToTable(table, i,1, state1.getTransactionID());    		
        	tm.prepareCommit(state1);
        	tm.doCommit(state1);
    	}
    	Assert.assertEquals(100, TxnTestUtils.countRow(table, 300, null));
    	table.flushCommits();
    	table.close();
    }

	@Ignore
    @Test
    public void testAbort() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
    	HTable table = TxnTestUtils.getTestTable(admin, testTable3);
    	TxnTestUtils.putDataToTable(table,500, 100, state1.getTransactionID());
    	Assert.assertEquals(100, TxnTestUtils.countRow(table, 500,state1.getTransactionID()));
    	tm.abort(state1);
    	Assert.assertFalse(rzk.exists(state1.getTransactionID(), false) != null);
    	Assert.assertEquals(0, TxnTestUtils.countRow(table, 500, null));
    }
    
    @Test
	public void oneSplit() throws Exception {
    	HTable table = getTestTable(admin, testTable1);
		TransactionState ts1 = tm.beginTransaction();
		TxnTestUtils.putDataToTable(table, 1, 100, ts1.getTransactionID());
		TxnTestUtils.putDataToTable(table, 20, 60, null);
		admin.split("TEST_TABLE1", "50");
		Assert.assertEquals(60, TxnTestUtils.countRow(table, null, null));
		tm.tryCommit(ts1);
		Assert.assertEquals(100, TxnTestUtils.countRow(table, null, null));
	}
	@Ignore
    @Test
    public void twoTxnSplit() throws Exception {
    	HTable table = getTestTable(admin, testTable1);
		TransactionState ts1 = tm.beginTransaction();
		TxnTestUtils.putDataToTable(table, 1, 80, ts1.getTransactionID());
		TransactionState ts2 = tm.beginTransaction();
		TxnTestUtils.putDataToTable(table, 20, 80, ts2.getTransactionID());
		TxnTestUtils.putDataToTable(table, 1, 70, null);
		admin.split("TEST_TABLE1", "ROW50");
		
    }
	
	@Ignore
    @Test
	public void testTxnLog1() throws Exception {
    	HTable table = getTestTable(admin, testTable1);
		TransactionState ts1 = tm.beginTransaction();
		TxnTestUtils.putDataToTable(table, 1, 2000, ts1.getTransactionID());
		TransactionState ts2 = tm.beginTransaction();
		TxnTestUtils.putDataToTable(table, 2101, 1900, ts2.getTransactionID());
		TxnTestUtils.putDataToTable(table, 1, 10, null);
		TxnTestUtils.putDataToTable(table, 2001, 100, null);
		TxnTestUtils.putDataToTable(table, 3991, 10, null);
		admin.split("TEST_TABLE1", "2050");
		tm.tryCommit(ts1);
		tm.tryCommit(ts2);
		Assert.assertEquals(4000, TxnTestUtils.countRow(table, null, null));
		printZK();
	}
	@Ignore
	@Test
	public void printZK() throws Exception {
		StringBuilder sb = new StringBuilder();
		for (String schemaPath : rzk.getChildren("/", false)) {
			schemaPath = "/" + schemaPath;
			sb.append(schemaPath + "\n");
			for (String table : rzk.getChildren(schemaPath, false)) {
				sb.append("   " + table + "\n");
				for (String family : rzk.getChildren(schemaPath + "/" + table, false)) {
					sb.append("      " + family + "\n");
					for (String column : rzk.getChildren(schemaPath + "/" + table + "/" + family, false)) {
						sb.append("         " + column + "\n");
					}
				}
			}
		}
		System.out.println(sb.toString());
	}
    
}