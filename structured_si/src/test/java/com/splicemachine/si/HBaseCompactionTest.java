package com.splicemachine.si;

import com.splicemachine.si.impl.SynchronousRollForwardQueue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.concurrent.Executors;

/**
 * @author Scott Fines
 * Date: 2/18/14
 */
public class HBaseCompactionTest extends CompactionTest {
		protected static String CLASS_NAME = HBaseAsyncRollForwardTest.class.getSimpleName();
		public HBaseCompactionTest() {
				useSimple=false;
		}

		@Override
		@Before
		public void setUp() throws Exception {
				SynchronousRollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);
				this.storeSetup = HBaseSuite.classStoreSetup;
				this.transactorSetup = HBaseSuite.classTransactorSetup;
				baseSetUp();
		}

		private static boolean selfManaged = false;
		@BeforeClass
		public static void setUpClass() throws Exception {
				if(HBaseSuite.classStoreSetup==null){
						System.out.printf("[%s]Not running in Suite, Setting up HBase myself%n", CLASS_NAME);
						HBaseSuite.setUp();
						selfManaged = true;
				}
		}

		@AfterClass
		public static void tearDownClass() throws Exception {
				if(selfManaged){
						System.out.printf("[%s]Tearing down HBase%n",CLASS_NAME);
						HBaseSuite.tearDownClass();
				}
		}
}
