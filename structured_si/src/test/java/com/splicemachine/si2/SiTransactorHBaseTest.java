package com.splicemachine.si2;

import com.splicemachine.si2.data.hbase.TransactorFactory;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.txn.TransactionManagerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class SiTransactorHBaseTest extends SiTransactorTest {

    private static HStoreSetup classStoreSetup;
    private static TransactorSetup classTransactorSetup;

    public SiTransactorHBaseTest() {
        useSimple = false;
    }

    @Override
    @Before
    public void setUp() {
        this.storeSetup = classStoreSetup;
        this.transactorSetup = classTransactorSetup;
        baseSetUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
    }

    @BeforeClass
    public static void setUpClass() {
        classStoreSetup = new HStoreSetup();
        classTransactorSetup = new TransactorSetup(classStoreSetup);
        Transactor transactor = classTransactorSetup.transactor;
        TransactorFactory.setDefaultTransactor(transactor);
        TransactionManagerFactory.setTransactor(transactor);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        classStoreSetup.getTestCluster().shutdownMiniCluster();
    }

}
