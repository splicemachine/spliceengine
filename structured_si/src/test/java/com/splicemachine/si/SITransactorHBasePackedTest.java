package com.splicemachine.si;

import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.SynchronousRollForwardQueue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Executors;

public class SITransactorHBasePackedTest extends SITransactorTest {

    private static HStoreSetup classStoreSetup;
    private static TransactorSetup classTransactorSetup;

    public SITransactorHBasePackedTest() {
        useSimple = false;
        usePacked = true;
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
        classStoreSetup = new HStoreSetup(true);
        classTransactorSetup = new TransactorSetup(classStoreSetup, false);
        HTransactorFactory.setTransactor(classTransactorSetup.hTransactor);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        classStoreSetup.getTestCluster().shutdownMiniCluster();
    }

    @Test
    public void writeScanWithDeleteActiveUncommittedAsOfStart() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeScanTwoVersionsUncommittedAsOfStart() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeReadViaFilterResult() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeScanWithManyDeletesUncommittedAsOfStart() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeScanMixSameRowUncommittedAsOfStart() throws IOException {
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
