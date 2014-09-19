package com.splicemachine.si.impl.store;

import com.splicemachine.si.SimpleTimestampSource;
import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.InMemoryTxnStore;
import com.splicemachine.si.impl.ReadOnlyTxn;
import com.splicemachine.si.impl.UnsupportedLifecycleManager;
import com.splicemachine.si.impl.WritableTxn;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.splicemachine.si.impl.TxnTestUtils.assertTxnsMatch;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that the ActiveTxnCacheSupplier will cache active transactions.
 *
 * @author Scott Fines
 * Date: 7/2/14
 */
public class ActiveTxnCacheTest {
		@Test
		public void testCachingWorks() throws Exception {
        TxnLifecycleManager tc = getLifecycleManager();
				Txn txn = new WritableTxn(1,1, Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tc, false);

				final boolean[] called = new boolean[]{false};
				TxnStore backStore = new InMemoryTxnStore(new SimpleTimestampSource(),Long.MAX_VALUE){
						@Override
						public Txn getTransaction(long txnId, boolean getDestinationTables) throws IOException {
								Assert.assertFalse("Item should have been fed from cache!", called[0]);
								called[0] = true;
								return super.getTransaction(txnId,getDestinationTables);
						}
				};
				backStore.recordNewTransaction(txn);

				TxnSupplier store = new ActiveTxnCacheSupplier(backStore,16);


				//fetch the transaction from the underlying store
				Assert.assertFalse("Cache thinks it already has the item!",store.transactionCached(txn.getTxnId()));

				TxnView fromStore = store.getTransaction(txn.getTxnId());
				assertTxnsMatch("Transaction from store is not correct!",txn,fromStore);

				Assert.assertTrue("Cache does not think it is present!",store.transactionCached(txn.getTxnId()));

				TxnView fromCache = store.getTransaction(txn.getTxnId());
				assertTxnsMatch("Transaction from store is not correct!",txn,fromCache);
		}

    @Test
    public void testDoesNotCacheRolledbackTransactions() throws Exception {
        TxnLifecycleManager tc = getLifecycleManager();
        Txn txn = new WritableTxn(1,1, Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tc, false);

        final AtomicInteger accessCount = new AtomicInteger(0);
        TxnStore backStore = new InMemoryTxnStore(new SimpleTimestampSource(),Long.MAX_VALUE){
            @Override
            public Txn getTransaction(long txnId, boolean getDestinationTables) throws IOException {
                accessCount.incrementAndGet();
                return super.getTransaction(txnId,getDestinationTables);
            }
        };
        backStore.recordNewTransaction(txn);
        txn.rollback();

        TxnSupplier store = new ActiveTxnCacheSupplier(backStore,16);

        //fetch the transaction from the underlying store
        Assert.assertFalse("Cache thinks it already has the item!",store.transactionCached(txn.getTxnId()));

        TxnView fromStore = store.getTransaction(txn.getTxnId());
        assertTxnsMatch("Transaction from store is not correct!",txn,fromStore);

        Assert.assertFalse("Cache thinks it is present!",store.transactionCached(txn.getTxnId()));

        TxnView fromCache = store.getTransaction(txn.getTxnId());
        assertTxnsMatch("Transaction from store is not correct!",txn,fromCache);

        //make sure that the access count is 2
        Assert.assertEquals("Did not access data from cache",2,accessCount.get());
    }

    @Test
    @Ignore
    public void testActiveCacheWillThrowAwaySoftReferences() throws Exception {
       /*
        * This is a test that the ActiveTxnCache won't hold on to so many references
        * that we can run out of memory. If ActiveTxnCache is working correctly, this should
        * run for forever (as the garbage collector will just collect away soft references as needed).
        * However, if it is not, we'll eventually see an OOM being thrown.
        */
        ActiveTxnCacheSupplier store = new ActiveTxnCacheSupplier(mock(TxnSupplier.class),1024);

        long txnId = 0;
        while(true){
            TxnView newTxn = ReadOnlyTxn.create(txnId, Txn.IsolationLevel.SNAPSHOT_ISOLATION, UnsupportedLifecycleManager.INSTANCE);
            store.cache(newTxn);
            txnId++;

            if((txnId % 1000000)==0){
                System.out.printf("Cached %d txns, giving a size of %d entries %n",txnId,store.getSize());
            }
        }
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    protected TxnLifecycleManager getLifecycleManager() throws IOException {
        final AtomicLong al = new AtomicLong(0l);
        TxnLifecycleManager tc = mock(TxnLifecycleManager.class);
        when(tc.commit(anyLong())).thenAnswer(new Answer<Long>(){

            @Override
            public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
                return al.incrementAndGet();
            }
        });
        return tc;
    }
}
