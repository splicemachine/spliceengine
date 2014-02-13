package com.splicemachine.si.txn;

import com.splicemachine.si.HStoreSetup;
import com.splicemachine.si.TestTransactionSetup;
import com.splicemachine.si.api.HTransactorFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class JtaXAResourceHBaseTest extends JtaXAResourceTest {
    @BeforeClass
    public static void setUp() {
        storeSetup = HStoreSetup.create();
        transactorSetup = new TestTransactionSetup(storeSetup, false);
        HTransactorFactory.setTransactor(transactorSetup.hTransactor);
        baseSetUp();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        HStoreSetup.destroy((HStoreSetup) storeSetup);
    }

}
