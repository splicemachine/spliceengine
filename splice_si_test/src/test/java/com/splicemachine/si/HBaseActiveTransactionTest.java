package com.splicemachine.si;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * HBase-backed test for Active Transactions
 */
public class HBaseActiveTransactionTest extends ActiveTransactionTest {
    private static boolean selfManaged = false;

    @BeforeClass
    public static void setUpClass() throws Exception {
        if (HBaseSuite.classStoreSetup == null) {
            System.out.printf("[%s]Not running in Suite, Setting up HBase myself%n", HBaseActiveTransactionTest.class.getSimpleName());
            HBaseSuite.setUp();
            selfManaged = true;
        }
        storeSetup = HBaseSuite.classStoreSetup;
        transactorSetup = new TestTransactionSetup(storeSetup, false);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (selfManaged) {
            System.out.printf("[%s]Tearing down HBase%n", HBaseActiveTransactionTest.class.getSimpleName());
            HBaseSuite.tearDownClass();
        }
    }

}