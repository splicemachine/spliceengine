package com.splicemachine.si.txn;

import com.splicemachine.si.HBaseSuite;
import com.splicemachine.si.api.HTransactorFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TransactionManagerHBaseTest extends TransactionManagerTest {
		private static boolean selfManaged = false;

		@BeforeClass
		public static void setUp() throws Exception {
				if(HBaseSuite.classStoreSetup==null){
						System.out.printf("[%s]Not running in Suite, Setting up HBase myself%n",TransactionManagerHBaseTest.class.getSimpleName());
						HBaseSuite.setUp();
						selfManaged=true;
				}
				storeSetup = HBaseSuite.classStoreSetup;
				transactorSetup = HBaseSuite.classTransactorSetup;
				HTransactorFactory.setTransactor(transactorSetup.hTransactor);
				baseSetUp();
		}

		@AfterClass
		public static void tearDown() throws Exception {
				if(selfManaged){
						System.out.printf("[%s]Tearing down HBase%n",TransactionManagerHBaseTest.class.getSimpleName());
						HBaseSuite.tearDownClass();
				}
		}

}
