package com.splicemachine.si;

import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.HTransactorFactory;
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
        classTransactorSetup = new TransactorSetup(classStoreSetup, false);
        classTransactor = classTransactorSetup.transactor;
        HTransactorFactory.setTransactor(classTransactorSetup.hTransactor);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        classStoreSetup.getTestCluster().shutdownMiniCluster();
    }

}
