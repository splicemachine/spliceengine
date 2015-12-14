package com.splicemachine.si;

import com.splicemachine.si.testsetup.SharedStoreHolder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class SIFilterHBaseTest extends SIFilterTest {

    public SIFilterHBaseTest() {
        useSimple = false;
    }

    @Before
    public void setUp() {
        storeSetup = SharedStoreHolder.getHstoreSetup();
        transactorSetup = SharedStoreHolder.getTestTransactionSetup();
        baseSetup();
    }

    @After
    public void tearDown() throws Exception {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

}