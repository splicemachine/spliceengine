package com.splicemachine.si.impl;

import com.splicemachine.si.SimpleTimestampSource;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnAccess;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnStore;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.splicemachine.si.impl.TxnTestUtils.assertTxnsMatch;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 * Date: 7/1/14
 */
public class CompletedTxnCacheAccessTest {

		@Test
		public void testDoesNotCacheActiveTransactions() throws Exception {
				final AtomicLong al = new AtomicLong(0l);
				TxnLifecycleManager tc = mock(TxnLifecycleManager.class);
				when(tc.commit(anyLong())).thenAnswer(new Answer<Long>(){

						@Override
						public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
								return al.incrementAndGet();
						}
				});
				Txn txn = new WritableTxn(1,1, Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tc,false,false);

				final boolean[] called = new boolean[]{false};
				TxnStore backStore = new InMemoryTxnStore(new SimpleTimestampSource(),Long.MAX_VALUE){
						@Override
						public Txn getTransaction(long txnId, boolean getDestinationTables) throws IOException {
								Assert.assertFalse("Item should have been fed from cache!",called[0]);
								called[0] = true;
								return super.getTransaction(txnId,getDestinationTables);
						}
				};
				backStore.recordNewTransaction(txn);

				TxnAccess store = new CompletedTxnCacheAccess(backStore,10,16);


				//fetch the transaction from the underlying store
				Assert.assertFalse("Cache thinks it already has the item!",store.transactionCached(txn.getTxnId()));

				Txn fromStore = store.getTransaction(txn.getTxnId());
				assertTxnsMatch("Transaction from store is not correct!",txn,fromStore);

				Assert.assertFalse("Cache does not think it is present!", store.transactionCached(txn.getTxnId()));
		}

		@Test
		public void testCachesRolledBackTransactions() throws Exception {
				final AtomicLong al = new AtomicLong(0l);
				TxnLifecycleManager tc = mock(TxnLifecycleManager.class);
				when(tc.commit(anyLong())).thenAnswer(new Answer<Long>(){

						@Override
						public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
								return al.incrementAndGet();
						}
				});
				Txn txn = new WritableTxn(1,1, Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tc,false,false);
				txn.rollback();

				final boolean[] called = new boolean[]{false};
				TxnStore backStore = new InMemoryTxnStore(new SimpleTimestampSource(),Long.MAX_VALUE){
						@Override
						public Txn getTransaction(long txnId, boolean getDestinationTables) throws IOException {
								Assert.assertFalse("Item should have been fed from cache!",called[0]);
								called[0] = true;
								return super.getTransaction(txnId,getDestinationTables);
						}
				};
				backStore.recordNewTransaction(txn);

				TxnAccess store = new CompletedTxnCacheAccess(backStore,10,16);


				//fetch the transaction from the underlying store
				Assert.assertFalse("Cache thinks it already has the item!",store.transactionCached(txn.getTxnId()));

				Txn fromStore = store.getTransaction(txn.getTxnId());
				assertTxnsMatch("Transaction from store is not correct!",txn,fromStore);

				Assert.assertTrue("Cache does not think it is present!",store.transactionCached(txn.getTxnId()));

				Txn fromCache = store.getTransaction(txn.getTxnId());
				assertTxnsMatch("Transaction from store is not correct!",txn,fromCache);
		}

		@Test
		public void testCachesCommittedTransactions() throws Exception {
				final AtomicLong al = new AtomicLong(0l);
				TxnLifecycleManager tc = mock(TxnLifecycleManager.class);
				when(tc.commit(anyLong())).thenAnswer(new Answer<Long>(){

						@Override
						public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
								return al.incrementAndGet();
						}
				});
				Txn txn = new WritableTxn(1,1, Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tc,false,false);
				txn.commit();

				final boolean[] called = new boolean[]{false};
				TxnStore backStore = new InMemoryTxnStore(new SimpleTimestampSource(),Long.MAX_VALUE){
						@Override
						public Txn getTransaction(long txnId, boolean getDestinationTables) throws IOException {
								Assert.assertFalse("Item should have been fed from cache!",called[0]);
								called[0] = true;
								return super.getTransaction(txnId,getDestinationTables);
						}
				};
				backStore.recordNewTransaction(txn);

				TxnAccess store = new CompletedTxnCacheAccess(backStore,10,16);


				//fetch the transaction from the underlying store
				Assert.assertFalse("Cache thinks it already has the item!",store.transactionCached(txn.getTxnId()));

				Txn fromStore = store.getTransaction(txn.getTxnId());
				assertTxnsMatch("Transaction from store is not correct!",txn,fromStore);

				Assert.assertTrue("Cache does not think it is present!",store.transactionCached(txn.getTxnId()));

				Txn fromCache = store.getTransaction(txn.getTxnId());
				assertTxnsMatch("Transaction from store is not correct!",txn,fromCache);
		}
}
