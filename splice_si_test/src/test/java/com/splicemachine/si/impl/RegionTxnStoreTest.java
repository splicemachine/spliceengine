package com.splicemachine.si.impl;

import com.splicemachine.impl.MockRegionUtils;
import com.splicemachine.si.api.SIFactory;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.si.impl.SparseTxn;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.si.impl.region.TransactionResolver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

/**
 * @author Scott Fines
 * Date: 6/30/14
 */
public class RegionTxnStoreTest extends TxnTestUtils {
		SIFactory factory = SIFactoryDriver.siFactory;

		@Test
		public void testCanWriteAndReadNewTransactionInformation() throws Exception {
				HRegion region = MockRegionUtils.getMockRegion();
				TransactionResolver resolver = getTransactionResolver();
				RegionTxnStore store = new RegionTxnStore(region,resolver,mock(TxnSupplier.class),SIFactoryDriver.siFactory.getDataLib(),SIFactoryDriver.siFactory.getTransactionLib());
				Object txn = factory.getTransaction(1,1,-1,-1,-1,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,"1234");
				factory.storeTransaction(store,txn);
				assertTxnsMatch("Transactions do not match!",txn,store.getTransaction(1));
		}


    @Test
		public void testCanCommitATransaction() throws Exception {
				HRegion region = MockRegionUtils.getMockRegion();
				RegionTxnStore store = new RegionTxnStore(region,getTransactionResolver(),mock(TxnSupplier.class),SIFactoryDriver.siFactory.getDataLib(),SIFactoryDriver.siFactory.getTransactionLib());
				Object txn = factory.getTransaction(1,1,-1,-1,-1,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,"1234");
				factory.storeTransaction(store,txn);
				Object transaction = store.getTransaction(1);
				assertTxnsMatch("Transactions do not match!",txn,transaction);
				Txn.State currentState = store.getState(1);
				Assert.assertEquals("Incorrect current state!",Txn.State.ACTIVE,currentState);
				long commitTs = 2l;
				store.recordCommit(1,commitTs);
				currentState = store.getState(1);
				Assert.assertEquals("Incorrect current state!",Txn.State.COMMITTED,currentState);

		}

		@Test
		public void testCanRollbackATransaction() throws Exception {
				HRegion region = MockRegionUtils.getMockRegion();
				RegionTxnStore store = new RegionTxnStore(region,getTransactionResolver(),mock(TxnSupplier.class),SIFactoryDriver.siFactory.getDataLib(),SIFactoryDriver.siFactory.getTransactionLib());
				Object txn = factory.getTransaction(1,1,-1,-1,-1,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,"12344");
				factory.storeTransaction(store,txn);
				Object transaction = store.getTransaction(1);
				assertTxnsMatch("Transactions do not match!",txn,transaction);
				store.recordRollback(1);
				Txn.State currentState = store.getState(1);
				Assert.assertEquals("Incorrect current state!",Txn.State.ROLLEDBACK,currentState);
		}

		@Test
		public void testCanGetActiveTransactions() throws Exception {
				HRegion region = MockRegionUtils.getMockRegion();
				RegionTxnStore store = new RegionTxnStore(region,getTransactionResolver(),mock(TxnSupplier.class),SIFactoryDriver.siFactory.getDataLib(),SIFactoryDriver.siFactory.getTransactionLib());
				Object txn = factory.getTransaction(1,1,-1,-1,-1,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,"12344");
				factory.storeTransaction(store,txn);
				long[] activeTxnIds = store.getActiveTxnIds(0, 2,null);
				Assert.assertEquals("Incorrect length!",1,activeTxnIds.length);
				Assert.assertArrayEquals("Incorrect listing!",new long[]{1},activeTxnIds);
		}

		@Test
		public void testGetActiveTransactionsFiltersOutRolledbackTxns() throws Exception {
				HRegion region = MockRegionUtils.getMockRegion();
				RegionTxnStore store = new RegionTxnStore(region,getTransactionResolver(),mock(TxnSupplier.class),SIFactoryDriver.siFactory.getDataLib(),SIFactoryDriver.siFactory.getTransactionLib());

				Object txn = factory.getTransaction(1,1,-1,-1,-1,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,"1234");
				factory.storeTransaction(store,txn);

				Thread.sleep(100); //sleep for 100 ms to ensure that the System.currentTimeMillis() moves forward
				store.recordRollback(1);
				long[] activeTxnIds = store.getActiveTxnIds(0, 2,null);
				Assert.assertEquals("Incorrect length!",0,activeTxnIds.length);
		}

		@Test
		public void testGetActiveTransactionsFiltersOutCommittedTxns() throws Exception {
				HRegion region = MockRegionUtils.getMockRegion();
				RegionTxnStore store = new RegionTxnStore(region,getTransactionResolver(),mock(TxnSupplier.class),SIFactoryDriver.siFactory.getDataLib(),SIFactoryDriver.siFactory.getTransactionLib());
				Object txn = factory.getTransaction(1,1,-1,-1,-1,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,"1234");
				factory.storeTransaction(store,txn);
				Thread.sleep(100); //sleep for 100 ms to ensure that the System.currentTimeMillis() moves forward
				store.recordCommit(1,2l);
				long[] activeTxnIds = store.getActiveTxnIds(0, 3,null);
				Assert.assertEquals("Incorrect length!",0,activeTxnIds.length);
		}

    protected TransactionResolver getTransactionResolver() {
        TransactionResolver resolver = mock(TransactionResolver.class);
        doNothing().when(resolver).resolveGlobalCommitTimestamp(any(HRegion.class), any(SparseTxn.class), anyBoolean());
        doNothing().when(resolver).resolveTimedOut(any(HRegion.class), any(SparseTxn.class), anyBoolean());
        return resolver;
    }

}
