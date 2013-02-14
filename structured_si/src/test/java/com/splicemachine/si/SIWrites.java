package com.splicemachine.si;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.impl.si.txn.Transaction;
import com.splicemachine.impl.si.txn.TransactionManagerImpl;
import com.splicemachine.si.filters.SIFilter;
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
		//tearDown();
	}
	
	@Test 
	public void singleWriteRecordTest() throws Exception {
		Transaction transaction = tm.beginTransaction();
		SIPut put = new SIPut(Bytes.toBytes(0),transaction.getStartTimestamp());
		put.add(SIConstants.DEFAULT_FAMILY, Bytes.toBytes(0), Bytes.toBytes(12));
		put.add(SIConstants.DEFAULT_FAMILY, Bytes.toBytes(1), Bytes.toBytes(12));
		put.add(SIConstants.DEFAULT_FAMILY, Bytes.toBytes(2), Bytes.toBytes(12));
		put.add(SIConstants.DEFAULT_FAMILY, Bytes.toBytes(3), Bytes.toBytes(12));
		HTableInterface siExample = pool.getTable(SI_EXAMPLE);
		siExample.put(put);
		tm.doCommit(transaction);
		Get get = new Get(Bytes.toBytes(0));
		get.setFilter(new SIFilter(0));
		Result result = siExample.get(get);
		System.out.println(result);
	}
	
}
