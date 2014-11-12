package com.splicemachine.si;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.impl.HTransactorFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Scott Fines
 * Date: 2/17/14
 */
@Suite.SuiteClasses({
				SIFilterHBaseTest.class,
				SITransactorHBasePackedTest.class,
        HBaseActiveTransactionTest.class,
        HBaseTransactionInteractionTest.class
})
@RunWith(Suite.class)
public class HBaseSuite {
		public static volatile HStoreSetup classStoreSetup;
		public static TestTransactionSetup classTransactorSetup;
		@BeforeClass
		public static void setUp() throws Exception {
				System.out.println("[HBaseSuite]:Setting up HBase for Tests");
				SpliceConstants.numRetries = 1;
				SIConstants.committingPause = 1000;
				classStoreSetup = new HStoreSetup(false);
				classTransactorSetup = new TestTransactionSetup(classStoreSetup, false);
				HTransactorFactory.setTransactor(classTransactorSetup.hTransactor);
        System.out.println("[HBaseSuite]: HBase setup complete");
		}

		@AfterClass
		public static void tearDownClass() throws Exception {
				System.out.println("[HBaseSuite]:Tearing down Hbase after tests");
				if(classStoreSetup!=null)
						classStoreSetup.shutdown();
		}
}
