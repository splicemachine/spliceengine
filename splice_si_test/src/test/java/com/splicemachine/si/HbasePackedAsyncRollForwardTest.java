package com.splicemachine.si;


import org.junit.Ignore;

/**
 * Packed version of HBase AsyncRollForwardTest
 * @author Scott Fines
 * Date: 2/17/14
 */
@Ignore("was not run by suites")
public class HbasePackedAsyncRollForwardTest extends HBaseAsyncRollForwardTest{
		static{
				CLASS_NAME = HbasePackedAsyncRollForwardTest.class.getSimpleName();
		}

		public HbasePackedAsyncRollForwardTest() {
				super();
		}


}
