package com.splicemachine.si;

import com.splicemachine.si.txn.JtaXAResourceHBaseTest;
import com.splicemachine.si.txn.TransactionManagerHBaseTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        TransactionManagerHBaseTest.class,
        JtaXAResourceHBaseTest.class,
        SIFilterHBaseTest.class,
        SITransactorHBaseTest.class
         })
public class SIHBaseSuite {

    @BeforeClass
    public static void setupSuite() {
        HStoreSetup.setUseSingleton(true);
        HStoreSetup.create();
    }

    @AfterClass
    public static void tearDownSuite() throws Exception {
        try {
            HStoreSetup.destroy(HStoreSetup.create());
        } finally {
            HStoreSetup.setUseSingleton(false);
        }
    }

}
