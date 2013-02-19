package com.splicemachine.si.utils;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.impl.si.txn.TransactionManagerImpl;
import com.splicemachine.si.hbase.SIGet;
import com.splicemachine.si.hbase.SIPut;
import com.splicemachine.si.hbase.SIScan;
import com.splicemachine.si.test.SIBaseTest;

public class SIUtilsTest extends SIBaseTest {
	public static final String timestampPath = "/SIUtilsTestPath";
	@Test
	public void generateSIHtableDescriptorTest() throws Exception {
		startup();
		tearDown();
	}
	
	@Test
	public void createWithParentsTest() throws Exception {
		TransactionManagerImpl tm = new TransactionManagerImpl();		
		SIUtils.createWithParents(tm.getRecoverableZooKeeper(), TxnConstants.DEFAULT_TRANSACTION_PATH);
	}
	
	@Test
	public void createIncreasingTimestampTest() throws Exception {
		TransactionManagerImpl tm = new TransactionManagerImpl();
		createWithParentsTest();
		SIUtils.createIncreasingTimestamp(TxnConstants.DEFAULT_TRANSACTION_PATH, tm.getRecoverableZooKeeper());
	}
	
	@Test
	public void shouldUseSITest() throws IOException {
		Scan scan = new Scan();
		SIScan scan2 = new SIScan(123l);
		Put put = new Put();
		SIPut put2 = new SIPut(Bytes.toBytes(1),1l);
		Get get = new Get();
		SIGet get2 = new SIGet(Bytes.toBytes(1),1l);
		Assert.assertTrue(!SIUtils.shouldUseSI(scan) && !SIUtils.shouldUseSI(put) && !SIUtils.shouldUseSI(get));
		Assert.assertTrue(SIUtils.shouldUseSI(scan2) && SIUtils.shouldUseSI(put2) && SIUtils.shouldUseSI(get2));
	}
	
	
	
}
