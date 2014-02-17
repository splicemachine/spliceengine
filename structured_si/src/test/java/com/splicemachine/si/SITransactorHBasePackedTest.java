package com.splicemachine.si;

import org.junit.*;

import java.io.IOException;

public class SITransactorHBasePackedTest extends SITransactorHBaseTest {

    public SITransactorHBasePackedTest() {
				super();
        usePacked = true;
    }

    @Override
    @Before
    public void setUp() {
        this.storeSetup = HBaseSuite.classStoreSetup;
        this.transactorSetup = HBaseSuite.classTransactorSetup;
        baseSetUp();
    }
		private static boolean selfManaged = false;
		@BeforeClass
		public static void setUpClass() throws Exception {
				if(HBaseSuite.classStoreSetup==null){
						System.out.printf("[%s]Not running in Suite, Setting up HBase myself%n",SITransactorHBasePackedTest.class.getSimpleName());
						HBaseSuite.setUp();
						selfManaged = true;
				}
		}

		@AfterClass
		public static void tearDownClass() throws Exception {
				if(selfManaged){
						System.out.printf("[%s]Tearing down HBase%n",SITransactorHBasePackedTest.class.getSimpleName());
						HBaseSuite.tearDownClass();
				}
		}

    @Test
    public void writeReadViaFilterResult() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeScanWithFilterAndPendingWrites() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeScanWithFilter() throws IOException {
        // temporarily mask test in parent class
    }

}
