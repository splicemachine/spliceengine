package com.splicemachine.si.txn;

import com.splicemachine.si.HStoreSetup;
import com.splicemachine.si.TransactorSetup;
import com.splicemachine.si.data.hbase.TransactorFactory;
import com.splicemachine.si.impl.TransactorFactoryImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TransactionManagerHBaseTest extends TransactionManagerTest {
    @BeforeClass
    public static void setUp() {
        storeSetup = new HStoreSetup();
        transactorSetup = new TransactorSetup(storeSetup);
        TransactorFactory.setDefaultTransactor(transactor);
        TransactorFactoryImpl.setTransactor(transactor);
        baseSetUp();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        storeSetup.getTestCluster().shutdownMiniCluster();
    }

}
