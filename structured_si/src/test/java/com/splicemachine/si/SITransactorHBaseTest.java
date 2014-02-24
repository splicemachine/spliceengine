package com.splicemachine.si;

import com.splicemachine.si.impl.SynchronousRollForwardQueue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.concurrent.Executors;

public class SITransactorHBaseTest extends SITransactorTest {

    public SITransactorHBaseTest() {
        useSimple = false;
    }

    @Override
    @Before
    public void setUp() {
        SynchronousRollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);
        baseSetUp();
    }

		private static boolean selfManaged = false;
		@BeforeClass
		public static void setUpClass() throws Exception {
				if(HBaseSuite.classStoreSetup==null){
						System.out.printf("[%s]Not running in Suite, Setting up HBase myself%n",SITransactorHBaseTest.class.getSimpleName());
						HBaseSuite.setUp();
						selfManaged = true;
				}
				storeSetup = HBaseSuite.classStoreSetup;
				transactorSetup = HBaseSuite.classTransactorSetup;
		}

		@AfterClass
		public static void tearDownClass() throws Exception {
				if(selfManaged){
						System.out.printf("[%s]Tearing down HBase%n",SITransactorHBaseTest.class.getSimpleName());
						HBaseSuite.tearDownClass();
				}
		}

}
