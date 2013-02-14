package com.splicemachine.si;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.si.test.SIBaseTest;

public class SIReads extends SIBaseTest {
	@BeforeClass
	public static void beforeClass() throws Exception {
		startup();
	}
	@AfterClass
	public static void afterClass() throws Exception {
		tearDown();
	}
	
	@Test 
	public void simpleCommittedRecordScanTest() throws Exception {
		
	}
}
