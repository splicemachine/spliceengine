package com.splicemachine.si;

import com.splicemachine.si.testsetup.SharedStoreHolder;
import com.splicemachine.si.testsetup.TestTransactionSetup;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class SITransactorHBasePackedTest extends SITransactorTest {

    public SITransactorHBasePackedTest() {
        this.useSimple = false;
    }

    @Override
    @Before
    public void setUp() throws IOException {
        storeSetup = SharedStoreHolder.getHstoreSetup();
        transactorSetup = new TestTransactionSetup(storeSetup, false);
        baseSetUp();
    }

    @BeforeClass
    public static void setUpClass() throws Exception {

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

    @Test
    public void writeWriteRead() throws IOException {
        super.writeWriteRead();
    }

    @Override
    public void testGetActiveTransactionsFiltersOutChildrenCommit() throws Exception {
        /*
         * We ignore this because it takes a very long time in HBase style, and we don't want
         * to hold up tests for it. Also, we have ITs around index creation that would break if
         * this doesn't work quite right, so in this case we're safe leaving off the underlying
         * submodule tests in favor of the larger module tests.
         */
    }

}
