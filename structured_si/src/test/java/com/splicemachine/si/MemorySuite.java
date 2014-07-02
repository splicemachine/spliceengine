package com.splicemachine.si;

import com.splicemachine.si.api.TransactionStatusTest;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.store.ActiveTxnCacheTest;
import com.splicemachine.si.impl.store.CompletedTxnCacheAccessTest;
import com.splicemachine.si.impl.translate.TranslatorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Scott Fines
 * Date: 2/17/14
 */
@Suite.SuiteClasses({
//				AsyncRollForwardTest.class,
//				PackedAsyncRollForwardTest.class,
//				LDataLibTest.class,
//				LStoreTest.class,
//				SIFilterTest.class,
				SimpleTxnFilterTest.class,
				SITransactorTest.class,
//				SITransactorPackedTest.class,
//				JtaXAResourceTest.class,
//				TransactionManagerTest.class,
//				ContiguousIteratorTest.class,
//				OrderedMuxerTest.class,
//				MemoryTableFactoryTest.class,
				TranslatorTest.class,
				CacheMapTest.class,
				TransactionIdTest.class,
				CompletedTxnCacheAccessTest.class,
				ActiveTxnCacheTest.class,
				RegionTxnStoreTest.class,
				SynchronousReadResolverTest.class,
//				CompactionTest.class,
                TransactionStatusTest.class
})
@RunWith(Suite.class)
public class MemorySuite{

}
