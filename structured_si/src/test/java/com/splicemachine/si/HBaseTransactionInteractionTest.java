package com.splicemachine.si;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 8/21/14
 */
public class HBaseTransactionInteractionTest extends TransactionInteractionTest {
    private static boolean selfManaged = false;

    @Override
    @Before
    public void setUp() throws IOException {
        this.storeSetup = HBaseSuite.classStoreSetup;
        this.transactorSetup = new TestTransactionSetup(storeSetup,false);
        useSimple = false;
        baseSetUp();
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        if(HBaseSuite.classStoreSetup==null){
            System.out.printf("[%s]Not running in Suite, Setting up HBase myself%n",HBaseTransactionInteractionTest.class.getSimpleName());
            HBaseSuite.setUp();
            selfManaged = true;
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if(selfManaged){
            System.out.printf("[%s]Tearing down HBase%n",HBaseTransactionInteractionTest.class.getSimpleName());
            HBaseSuite.tearDownClass();
        }
    }

}
