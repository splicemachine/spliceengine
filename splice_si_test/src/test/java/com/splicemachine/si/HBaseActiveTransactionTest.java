package com.splicemachine.si;

import com.splicemachine.si.testsetup.SharedStoreHolder;
import com.splicemachine.si.testsetup.TestTransactionSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * HBase-backed test for Active Transactions
 */
public class HBaseActiveTransactionTest extends ActiveTransactionTest {

    @BeforeClass
    public static void setUpClass() throws Exception {
        storeSetup = SharedStoreHolder.getHstoreSetup();
        transactorSetup = new TestTransactionSetup(storeSetup, false);
    }

}