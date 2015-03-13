package com.splicemachine.si;

import com.splicemachine.si.testsetup.SharedStoreHolder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * @author Scott Fines
 *         Date: 2/18/14
 */
public abstract class HBaseCompactionTest extends CompactionTest {

    protected static String CLASS_NAME = HBaseAsyncRollForwardTest.class.getSimpleName();

    public HBaseCompactionTest() {
        useSimple = false;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        this.storeSetup = SharedStoreHolder.getHstoreSetup();
        this.transactorSetup = SharedStoreHolder.getTestTransactionSetup();
        baseSetUp();
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

}
