package com.splicemachine.si;

import com.splicemachine.si.data.hbase.TransactorFactory;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.TransactorFactoryImpl;
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
        TransactorFactoryImpl.setTransactor(transactor);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        classStoreSetup.getTestCluster().shutdownMiniCluster();
    }

}
