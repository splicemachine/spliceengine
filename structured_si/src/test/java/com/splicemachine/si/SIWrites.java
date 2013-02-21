package com.splicemachine.si;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.impl.si.txn.Transaction;
import com.splicemachine.impl.si.txn.TransactionManagerImpl;
import com.splicemachine.si.hbase.SIGet;
import com.splicemachine.si.hbase.SIPut;
import com.splicemachine.si.hbase.SIScan;
import com.splicemachine.si.test.SIBaseTest;
import com.splicemachine.si.utils.SIConstants;

public class SIWrites extends SIBaseTest {
	protected static TransactionManagerImpl tm;
	protected static int increment = 0;
	private synchronized int incrementValue() {
		return increment++;
	}
	@BeforeClass
	public static void beforeClass() throws Exception {
		startup();
		tm = new TransactionManagerImpl();
}
	@AfterClass
	public static void afterClass() throws Exception {
		//tearDown();
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
		int first = incrementValue();
		int second = incrementValue();
		siExample.put(generatePut(transaction,Bytes.toBytes(first),Bytes.toBytes(12)));
		siExample.put(generatePut(transaction,Bytes.toBytes(second),Bytes.toBytes(12)));	
		tm.doCommit(transaction);
		Transaction lateTransaction = tm.beginTransaction();
		Result result = siExample.get(new SIGet(Bytes.toBytes(first),earlyTransaction.getStartTimestamp()));
		Assert.assertTrue(result.isEmpty());
		Result result2 = siExample.get(new SIGet(Bytes.toBytes(second),lateTransaction.getStartTimestamp()));
		Assert.assertNotNull(result2);
	}
	
	@Test 
	public void writeWriteConflictTest() throws Exception {
		Transaction earlyTransaction = tm.beginTransaction();
		Transaction transaction = tm.beginTransaction();
		HTableInterface siExample = pool.getTable(SI_EXAMPLE);
		int first = incrementValue();
		siExample.put(generatePut(earlyTransaction,Bytes.toBytes(first),Bytes.toBytes(12)));	
		try {
			siExample.put(generatePut(transaction,Bytes.toBytes(first),Bytes.toBytes(12)));		
			Assert.fail("Needed to throw a RetriesExhaustedWithDetailsException with a DoNotRetryIOException cause");
		} catch (RetriesExhaustedWithDetailsException retriesExhausted) {
			Assert.assertTrue(retriesExhausted.getCauses().get(0).getClass() == DoNotRetryIOException.class);
		}
	}
	
	@Test 
	public void abortTest() throws Exception {
		Transaction transaction = tm.beginTransaction();
		HTableInterface siExample = pool.getTable(SI_EXAMPLE);
		int first = incrementValue();
		siExample.put(generatePut(transaction,Bytes.toBytes(first),Bytes.toBytes(12)));	
		tm.abort(transaction);
		Transaction nextTransaction = tm.beginTransaction();
		Assert.assertTrue(siExample.get(new SIGet(Bytes.toBytes(first),nextTransaction.getStartTimestamp())).isEmpty());
		ResultScanner resultScanner = siExample.getScanner(new Scan());
		Result result;
		int i = 0;
		while ( (result = resultScanner.next() ) != null) {
			i++;
		}
		Assert.assertEquals(1, i);
		i = 0;
		resultScanner = siExample.getScanner(new SIScan(nextTransaction.getStartTimestamp()));
		while ( (result = resultScanner.next() ) != null) {
			i++;
		}
		Assert.assertEquals(0, i);
	}
	//@Before
	public void cleanupOldValues() throws IOException {
		HTableInterface siExample = pool.getTable(SI_EXAMPLE);
		ResultScanner resultScanner = siExample.getScanner(new Scan());
		Result result;
		while ( (result = resultScanner.next() ) != null) {
			System.out.println("Cleaning up old values: " + result);
			siExample.delete(new Delete(result.getRow()));
		}
	}
}
