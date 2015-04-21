package com.splicemachine.si.impl;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.impl.MockRegionUtils;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.si.testsetup.SimpleTimestampSource;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnStore;
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.utils.GreenLight;
import com.splicemachine.utils.TrafficControl;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;

/**
 * Tests around the possibilities for the SynchronousReadResolver
 * @author Scott Fines
 *         Date: 7/2/14
 */
public class SynchronousReadResolverTest {

		@Test
		public void testResolveRolledBackWorks() throws Exception {
				HRegion region = MockRegionUtils.getMockRegion();
				TrafficControl control = GreenLight.INSTANCE;

        final TxnStore store = new InMemoryTxnStore(new SimpleTimestampSource(),Long.MAX_VALUE);
				ReadResolver resolver = SynchronousReadResolver.getResolver(region,store,new RollForwardStatus(), control, false);
        final IgnoreTxnCacheSupplier ignoreTxnCacheSupplier = new IgnoreTxnCacheSupplier();
        TxnLifecycleManager tc = mock(TxnLifecycleManager.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                store.rollback((Long)invocationOnMock.getArguments()[0]);
                return null;
            }
        }).when(tc).rollback(1l);

				Txn rolledBackTxn = new WritableTxn(1l,1l, Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tc, false);
				store.recordNewTransaction(rolledBackTxn);
        rolledBackTxn.rollback(); //ensure that it's rolled back

				byte[] rowKey = Encoding.encode("hello");
				Put testPut = new Put(rowKey);
				testPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES,
								SpliceConstants.PACKED_COLUMN_BYTES,
								rolledBackTxn.getTxnId(),Encoding.encode("hello2"));

				region.put(testPut);

				Txn readTxn = ReadOnlyTxn.createReadOnlyTransaction(2l,Txn.ROOT_TRANSACTION,2l,
								Txn.IsolationLevel.SNAPSHOT_ISOLATION, false,mock(TxnLifecycleManager.class));
				SimpleTxnFilter filter = new SimpleTxnFilter(null,store,ignoreTxnCacheSupplier,readTxn,resolver,TxnTestUtils.getMockDataStore());

				Result result = region.get(new Get(rowKey));
				Assert.assertEquals("Incorrect result size", 1, result.size());
				KeyValue kv = result.getColumnLatest(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES);
				Assert.assertNotNull("No data column found!",kv);

				Filter.ReturnCode returnCode = filter.filterKeyValue(kv);
				Assert.assertEquals("Incorrect return code!", Filter.ReturnCode.SKIP,returnCode);

				//check to see if the resolver added the proper key value
				result = region.get(new Get(rowKey));
				Assert.assertEquals("Incorrect result size after read resolve!",0,result.size());
		}

		@Test
		public void testResolvingCommittedWorks() throws Exception {
				HRegion region = MockRegionUtils.getMockRegion();

        final SimpleTimestampSource commitTsGenerator = new SimpleTimestampSource();
        final TxnStore store = new InMemoryTxnStore(commitTsGenerator,Long.MAX_VALUE);
				ReadResolver resolver = SynchronousReadResolver.getResolver(region,store,new RollForwardStatus(),GreenLight.INSTANCE,false);
        final IgnoreTxnCacheSupplier ignoreTxnCacheSupplier = new IgnoreTxnCacheSupplier();
        TxnLifecycleManager tc = mock(TxnLifecycleManager.class);
        doAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
                long next = commitTsGenerator.nextTimestamp();
                store.commit((Long)invocationOnMock.getArguments()[0]);
                return next+1;
            }
        }).when(tc).commit(anyLong());
				Txn committedTxn = new WritableTxn(1l,1l, Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tc, false);
				store.recordNewTransaction(committedTxn);
        committedTxn.commit();

				byte[] rowKey = Encoding.encode("hello");
				Put testPut = new Put(rowKey);
				testPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES,
								SpliceConstants.PACKED_COLUMN_BYTES,
								committedTxn.getTxnId(),Encoding.encode("hello2"));

				region.put(testPut);

				Txn readTxn = ReadOnlyTxn.createReadOnlyTransaction(3l,Txn.ROOT_TRANSACTION,3l,
								Txn.IsolationLevel.SNAPSHOT_ISOLATION, false,mock(TxnLifecycleManager.class));
				SimpleTxnFilter filter = new SimpleTxnFilter(null,store,ignoreTxnCacheSupplier,readTxn,resolver,TxnTestUtils.getMockDataStore());

				Result result = region.get(new Get(rowKey));
				Assert.assertEquals("Incorrect result size", 1, result.size());
				KeyValue kv = result.getColumnLatest(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES);
				Assert.assertNotNull("No data column found!",kv);

				Filter.ReturnCode returnCode = filter.filterKeyValue(kv);
				Assert.assertEquals("Incorrect return code!", Filter.ReturnCode.INCLUDE,returnCode);

				//check to see if the resolver added the proper key value
				result = region.get(new Get(rowKey));
				Assert.assertEquals("Incorrect result size after read resolve!",2,result.size());
				KeyValue commitTs = result.getColumnLatest(SpliceConstants.DEFAULT_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES);
				Assert.assertNotNull("No Commit TS column found!",commitTs);
				Assert.assertEquals("Incorrect committed txnId",committedTxn.getTxnId(),commitTs.getTimestamp());
				Assert.assertEquals("Incorrect commit timestamp!",committedTxn.getEffectiveCommitTimestamp(), Bytes.toLong(commitTs.getValue()));
		}

		@Test
		public void testResolvingCommittedDoesNotHappenUntilParentCommits() throws Exception {
				HRegion region = MockRegionUtils.getMockRegion();

        SimpleTimestampSource timestampSource = new SimpleTimestampSource();
        final IgnoreTxnCacheSupplier ignoreTxnCacheSupplier = new IgnoreTxnCacheSupplier();
        TxnStore store = new InMemoryTxnStore(timestampSource,Long.MAX_VALUE);
				ReadResolver resolver = SynchronousReadResolver.getResolver(region,store,new RollForwardStatus(),GreenLight.INSTANCE,false);

				ClientTxnLifecycleManager tc = new ClientTxnLifecycleManager(timestampSource);
				tc.setStore(store);
				tc.setKeepAliveScheduler(new ManualKeepAliveScheduler(store));
				Txn parentTxn = tc.beginTransaction(Bytes.toBytes("1184"));

				Txn childTxn = tc.beginChildTransaction(parentTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION, false,Bytes.toBytes("1184"));

				byte[] rowKey = Encoding.encode("hello");
				Put testPut = new Put(rowKey);
				testPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES,
								SpliceConstants.PACKED_COLUMN_BYTES,
								childTxn.getTxnId(),Encoding.encode("hello2"));

				region.put(testPut);

				childTxn.commit();

				Txn readTxn = tc.beginTransaction(); //a read-only transaction with SI semantics
				SimpleTxnFilter filter = new SimpleTxnFilter(null,store,ignoreTxnCacheSupplier,readTxn,resolver,TxnTestUtils.getMockDataStore());

				Result result = region.get(new Get(rowKey));
				Assert.assertEquals("Incorrect result size", 1, result.size());
				KeyValue kv = result.getColumnLatest(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES);
				Assert.assertNotNull("No data column found!",kv);

				Filter.ReturnCode returnCode = filter.filterKeyValue(kv);
				Assert.assertEquals("Incorrect return code!", Filter.ReturnCode.SKIP,returnCode);

				//make sure the resolver has not added anything
				result = region.get(new Get(rowKey));
				Assert.assertEquals("Incorrect result size after read resolve!",1,result.size());

				//commit the parent and see if resolution works then
				parentTxn.commit();

				//now re-read the data and make sure that it resolves
				filter.nextRow();
				result = region.get(new Get(rowKey));
				Assert.assertEquals("Incorrect result size", 1, result.size());
				kv = result.getColumnLatest(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES);
				Assert.assertNotNull("No data column found!",kv);

				returnCode = filter.filterKeyValue(kv);
				Assert.assertEquals("Incorrect return code!", Filter.ReturnCode.SKIP,returnCode);

				//make sure that the read-resolver worked
				result = region.get(new Get(rowKey));
				Assert.assertEquals("Incorrect result size", 2, result.size());
				kv = result.getColumnLatest(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES);
				Assert.assertNotNull("No data column found!",kv);
				KeyValue commitTs = result.getColumnLatest(SpliceConstants.DEFAULT_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES);
				Assert.assertNotNull("No Commit TS column found!",commitTs);
				Assert.assertEquals("Incorrect committed txnId",childTxn.getTxnId(),commitTs.getTimestamp());
				Assert.assertEquals("Incorrect commit timestamp!",childTxn.getEffectiveCommitTimestamp(), Bytes.toLong(commitTs.getValue()));
		}
}
