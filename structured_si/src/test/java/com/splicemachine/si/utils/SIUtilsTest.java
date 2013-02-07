package com.splicemachine.si.utils;

import org.junit.Test;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.impl.si.txn.TransactionManagerImpl;
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
}
