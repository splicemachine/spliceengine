package com.splicemachine.si;

import com.splicemachine.si.api.TransactionStatusTest;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.store.ActiveTxnCacheTest;
import com.splicemachine.si.impl.store.CompletedTxnCacheAccessTest;
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
				CompletedTxnCacheAccessTest.class,
				ActiveTxnCacheTest.class,
				RegionTxnStoreTest.class,
				SynchronousReadResolverTest.class,
        ActiveTransactionTest.class,
        TransactionStatusTest.class
})
@RunWith(Suite.class)
public class MemorySuite{

}
