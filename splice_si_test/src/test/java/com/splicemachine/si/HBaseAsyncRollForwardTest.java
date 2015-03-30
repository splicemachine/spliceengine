package com.splicemachine.si;

import com.splicemachine.si.testsetup.SharedStoreHolder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * HBase version of AsyncRollForwardTest
 *
 * @author Scott Fines
 *         Date: 2/17/14
 */
public abstract class HBaseAsyncRollForwardTest extends AsyncRollForwardTest {
    protected static String CLASS_NAME = HBaseAsyncRollForwardTest.class.getSimpleName();

    public HBaseAsyncRollForwardTest() {
        useSimple = false;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        this.storeSetup = SharedStoreHolder.getHstoreSetup();
        this.transactorSetup = SharedStoreHolder.getTestTransactionSetup();
        baseSetUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

}
