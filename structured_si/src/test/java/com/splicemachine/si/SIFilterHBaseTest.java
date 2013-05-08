package com.splicemachine.si;

import com.splicemachine.si.data.hbase.TransactorFactory;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.TransactorFactoryImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class SIFilterHBaseTest extends SIFilterTest {
    static StoreSetup classStoreSetup;
    static TransactorSetup classTransactorSetup;
    static Transactor classTransactor;

    public SIFilterHBaseTest() {
        useSimple = false;
    }

    @Before
    public void setUp() {
        storeSetup = classStoreSetup;
        transactorSetup = classTransactorSetup;
        transactor = classTransactor;
    }

    @After
    public void tearDown() throws Exception {
    }

    @BeforeClass
    public static void setUpClass() {
        classStoreSetup = new HStoreSetup();
        classTransactorSetup = new TransactorSetup(classStoreSetup);
        classTransactor = classTransactorSetup.transactor;
        TransactorFactory.setDefaultTransactor(classTransactor);
        TransactorFactoryImpl.setTransactor(classTransactor);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        classStoreSetup.getTestCluster().shutdownMiniCluster();
    }

}
