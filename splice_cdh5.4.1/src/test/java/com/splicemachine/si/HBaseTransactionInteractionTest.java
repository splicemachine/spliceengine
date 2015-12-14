package com.splicemachine.si;

import com.splicemachine.si.testsetup.SharedStoreHolder;
import com.splicemachine.si.testsetup.TestTransactionSetup;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 8/21/14
 */
public class HBaseTransactionInteractionTest extends TransactionInteractionTest {

    @Override
    @Before
    public void setUp() throws IOException {
        storeSetup = SharedStoreHolder.getHstoreSetup();
        transactorSetup = new TestTransactionSetup(storeSetup, false);
        useSimple = false;
        baseSetUp();
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

}