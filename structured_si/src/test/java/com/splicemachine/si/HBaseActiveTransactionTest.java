package com.splicemachine.si;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * HBase-backed test for Active Transactions
 * @author Scott Fines
 * Date: 8/21/14
 */
public class HBaseActiveTransactionTest extends ActiveTransactionTest{
    private static boolean selfManaged = false;

    @Override
    @Before
    public void setUp() throws IOException {
        this.storeSetup = HBaseSuite.classStoreSetup;
        this.transactorSetup = new TestTransactionSetup(storeSetup,false);
        baseSetUp();
    }


    @BeforeClass
    public static void setUpClass() throws Exception {
        if(HBaseSuite.classStoreSetup==null){
            System.out.printf("[%s]Not running in Suite, Setting up HBase myself%n",HBaseActiveTransactionTest.class.getSimpleName());
            HBaseSuite.setUp();
            selfManaged = true;
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if(selfManaged){
            System.out.printf("[%s]Tearing down HBase%n",HBaseActiveTransactionTest.class.getSimpleName());
            HBaseSuite.tearDownClass();
        }
    }
}
