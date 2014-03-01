package com.splicemachine.si;

import com.splicemachine.si.impl.SynchronousRollForwardQueue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.concurrent.Executors;

/**
 * Packed version of HBase AsyncRollForwardTest
 * @author Scott Fines
 * Date: 2/17/14
 */
public class HbasePackedAsyncRollForwardTest extends HBaseAsyncRollForwardTest{
		static{
				CLASS_NAME = HbasePackedAsyncRollForwardTest.class.getSimpleName();
		}

		public HbasePackedAsyncRollForwardTest() {
				super();
		}


}
