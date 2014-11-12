package com.splicemachine.si;

import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.store.ActiveTxnCacheTest;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplierTest;
import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Scott Fines
 * Date: 2/17/14
 */
@Suite.SuiteClasses({
				SimpleTxnFilterTest.class,
				SITransactorTest.class,
        TransactionInteractionTest.class,
				CompletedTxnCacheSupplierTest.class,
				ActiveTxnCacheTest.class,
				RegionTxnStoreTest.class,
				SynchronousReadResolverTest.class,
        ActiveTransactionTest.class,
})
@RunWith(Suite.class)
public class MemorySuite{

    @AfterClass
    public static void tearDown() throws Exception {
        //clear any transactional stuff
    }
}
