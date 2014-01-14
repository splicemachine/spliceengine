package com.splicemachine.si.txn;

import com.splicemachine.si.HStoreSetup;
import com.splicemachine.si.TransactorSetup;
import com.splicemachine.si.api.HTransactorFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TransactionManagerHBaseTest extends TransactionManagerTest {
    @BeforeClass
    public static void setUp() {
        storeSetup = HStoreSetup.create();
        transactorSetup = new TransactorSetup(storeSetup, false);
        HTransactorFactory.setTransactor(transactorSetup.hTransactor);
        baseSetUp();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        HStoreSetup.destroy((HStoreSetup) storeSetup);
    }

}
