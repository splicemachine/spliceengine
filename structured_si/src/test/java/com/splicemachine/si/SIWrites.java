package com.splicemachine.si;

import junit.framework.Assert;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.impl.si.txn.Transaction;
import com.splicemachine.impl.si.txn.TransactionManagerImpl;
import com.splicemachine.si.hbase.SIGet;
import com.splicemachine.si.hbase.SIPut;
import com.splicemachine.si.test.SIBaseTest;
import com.splicemachine.si.utils.SIConstants;

public class SIWrites extends SIBaseTest {
	protected static TransactionManagerImpl tm;
	
	@BeforeClass
	public static void beforeClass() throws Exception {
		startup();
		tm = new TransactionManagerImpl();
}
	@AfterClass
	public static void afterClass() throws Exception {
		tearDown();
	}

	private static Put generatePut(Transaction transaction, byte[] rowKey, byte[] value) {
		SIPut put = new SIPut(rowKey,transaction.getStartTimestamp());
		put.add(SIConstants.DEFAULT_FAMILY, Bytes.toBytes(0), value);
		put.add(SIConstants.DEFAULT_FAMILY, Bytes.toBytes(1), value);
		put.add(SIConstants.DEFAULT_FAMILY, Bytes.toBytes(2), value);
		put.add(SIConstants.DEFAULT_FAMILY, Bytes.toBytes(3), value);
		return put;
	}

	@Test 
	public void singleWriteRecordTest() throws Exception {
		Transaction earlyTransaction = tm.beginTransaction();
		Transaction transaction = tm.beginTransaction();
		HTableInterface siExample = pool.getTable(SI_EXAMPLE);
		siExample.put(generatePut(transaction,Bytes.toBytes(0),Bytes.toBytes(12)));
		siExample.put(generatePut(transaction,Bytes.toBytes(1),Bytes.toBytes(12)));	
		HTableUtil util;
		tm.doCommit(transaction);
		Transaction lateTransaction = tm.beginTransaction();
		Result result = siExample.get(new SIGet(Bytes.toBytes(0),earlyTransaction.getStartTimestamp()));
		Assert.assertTrue(result.isEmpty());
		Result result2 = siExample.get(new SIGet(Bytes.toBytes(0),lateTransaction.getStartTimestamp()));
		Assert.assertNotNull(result2);
	}
	
	@Test 
	public void writeWriteConflictTest() throws Exception {
		Transaction earlyTransaction = tm.beginTransaction();
		Transaction transaction = tm.beginTransaction();
		HTableInterface siExample = pool.getTable(SI_EXAMPLE);
		siExample.put(generatePut(earlyTransaction,Bytes.toBytes(0),Bytes.toBytes(12)));	
		siExample.put(generatePut(transaction,Bytes.toBytes(0),Bytes.toBytes(12)));		
	}
}
