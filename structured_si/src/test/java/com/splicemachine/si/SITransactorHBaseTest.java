package com.splicemachine.si;

import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.RollForwardQueue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.concurrent.Executors;

public class SITransactorHBaseTest extends SITransactorTest {

    private static HStoreSetup classStoreSetup;
    private static TransactorSetup classTransactorSetup;

    public SITransactorHBaseTest() {
        useSimple = false;
    }

    @Override
    @Before
    public void setUp() {
        RollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);
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
        classStoreSetup = new HStoreSetup(false);
        classTransactorSetup = new TransactorSetup(classStoreSetup, false);
        Transactor transactor = classTransactorSetup.transactor;
        HTransactorFactory.setTransactor(classTransactorSetup.hTransactor);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        classStoreSetup.getTestCluster().shutdownMiniCluster();
    }

}
