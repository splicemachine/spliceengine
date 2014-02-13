package com.splicemachine.si;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.HTransactorFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class SITransactorHBasePackedTest extends SITransactorTest {

    private static HStoreSetup classStoreSetup;
    private static TestTransactionSetup classTransactorSetup;

    public SITransactorHBasePackedTest() {
        useSimple = false;
        usePacked = true;
    }

    @Override
    @Before
    public void setUp() {
//        SynchronousRollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);

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
				SpliceConstants.numRetries = 2;
				SIConstants.transactionTimeout = 500;
        classStoreSetup = new HStoreSetup(true);
        classTransactorSetup = new TestTransactionSetup(classStoreSetup, false);
        HTransactorFactory.setTransactor(classTransactorSetup.hTransactor);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        classStoreSetup.getTestCluster().shutdownMiniCluster();
    }

    @Test
    public void writeReadViaFilterResult() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeScanWithFilterAndPendingWrites() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeScanWithFilter() throws IOException {
        // temporarily mask test in parent class
    }

}
