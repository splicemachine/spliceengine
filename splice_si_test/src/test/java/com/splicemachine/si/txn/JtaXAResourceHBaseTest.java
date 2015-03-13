package com.splicemachine.si.txn;

import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.testsetup.SharedStoreHolder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

@Ignore("was not run by suites")
public class JtaXAResourceHBaseTest extends JtaXAResourceTest {

    @BeforeClass
    public static void setUp() throws Exception {
        storeSetup = SharedStoreHolder.getHstoreSetup();
        transactorSetup = SharedStoreHolder.getTestTransactionSetup();
        HTransactorFactory.setTransactor(transactorSetup.hTransactor);
        baseSetUp();
    }
}
