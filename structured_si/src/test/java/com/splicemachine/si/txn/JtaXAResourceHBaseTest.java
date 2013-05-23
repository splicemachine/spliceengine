package com.splicemachine.si.txn;

import com.splicemachine.si.HStoreSetup;
import com.splicemachine.si.TransactorSetup;
import com.splicemachine.si.api.com.splicemachine.si.api.hbase.HTransactorFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class JtaXAResourceHBaseTest extends JtaXAResourceTest {
    @BeforeClass
    public static void setUp() {
        storeSetup = new HStoreSetup();
        transactorSetup = new TransactorSetup(storeSetup, false);
        HTransactorFactory.setTransactor(transactorSetup.hTransactor);
        baseSetUp();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        storeSetup.getTestCluster().shutdownMiniCluster();
    }

}
