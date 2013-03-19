package com.splicemachine.si2.txn;

import com.splicemachine.si2.HStoreSetup;
import com.splicemachine.si2.TransactorSetup;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TransactionManagerHBaseTest extends TransactionManagerTest {
    @BeforeClass
    public static void setUp() {
        storeSetup = new HStoreSetup();
        transactorSetup = new TransactorSetup(storeSetup);
        TransactorFactory.setDefaultTransactor(transactor);
        TransactionManagerFactory.setTransactor(transactor);
        baseSetUp();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        storeSetup.getTestCluster().shutdownMiniCluster();
    }

}
