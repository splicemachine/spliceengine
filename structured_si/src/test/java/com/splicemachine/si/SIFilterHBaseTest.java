package com.splicemachine.si;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class SIFilterHBaseTest extends SIFilterTest {

    public SIFilterHBaseTest() {
        useSimple = false;
    }

    @Before
    public void setUp() {
        storeSetup = HBaseSuite.classStoreSetup;
        transactorSetup = HBaseSuite.classTransactorSetup;
				baseSetup();
    }

    @After
    public void tearDown() throws Exception {
    }

		private static boolean selfManaged = false;
		@BeforeClass
		public static void setUpClass() throws Exception {
				if(HBaseSuite.classStoreSetup==null){
						System.out.printf("[%s]Not running in Suite, Setting up HBase myself%n", SIFilterHBaseTest.class.getSimpleName());
						HBaseSuite.setUp();
						selfManaged = true;
				}
		}

		@AfterClass
		public static void tearDownClass() throws Exception {
				if(selfManaged){
						System.out.printf("[%s]Tearing down HBase%n",SIFilterHBaseTest.class.getSimpleName());
						HBaseSuite.tearDownClass();
				}
		}

}
