package com.splicemachine.si;

import com.splicemachine.si.api.TransactionStatusTest;
import com.splicemachine.si.impl.CacheMapTest;
import com.splicemachine.si.impl.TransactionIdTest;
import com.splicemachine.si.impl.iterator.ContiguousIteratorTest;
import com.splicemachine.si.impl.iterator.OrderedMuxerTest;
import com.splicemachine.si.impl.rollforward.SIRollForwardQueueTest;
import com.splicemachine.si.impl.translate.MemoryTableFactoryTest;
import com.splicemachine.si.impl.translate.TranslatorTest;
import com.splicemachine.si.txn.JtaXAResourceTest;
import com.splicemachine.si.txn.TransactionManagerTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Scott Fines
 * Date: 2/17/14
 */
@Suite.SuiteClasses({AsyncRollForwardTest.class,
				PackedAsyncRollForwardTest.class,
				LDataLibTest.class,
				LStoreTest.class,
				SIFilterTest.class,
				SITransactorTest.class,
				SITransactorPackedTest.class,
				JtaXAResourceTest.class,
				TransactionManagerTest.class,
				ContiguousIteratorTest.class,
				OrderedMuxerTest.class,
				MemoryTableFactoryTest.class,
				TranslatorTest.class,
				CacheMapTest.class,
				SIRollForwardQueueTest.class,
				TransactionIdTest.class,
				CompactionTest.class,
                TransactionStatusTest.class
})
@RunWith(Suite.class)
public class MemorySuite{

}
