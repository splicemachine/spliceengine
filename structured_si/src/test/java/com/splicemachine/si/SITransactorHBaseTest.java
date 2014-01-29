package com.splicemachine.si;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.SynchronousRollForwardQueue;
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
        SynchronousRollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);
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
				SpliceConstants.numRetries = 1;
				SIConstants.committingPause = 1000;
        classStoreSetup = HStoreSetup.create();
        classTransactorSetup = new TransactorSetup(classStoreSetup, false);
        Transactor transactor = classTransactorSetup.transactor;
        HTransactorFactory.setTransactor(classTransactorSetup.hTransactor);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        HStoreSetup.destroy(classStoreSetup);
    }

}
