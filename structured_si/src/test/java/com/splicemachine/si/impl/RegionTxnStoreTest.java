package com.splicemachine.si.impl;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.splicemachine.hbase.ByteBufferArrayUtils;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.si.api.Txn;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static com.splicemachine.si.impl.TxnTestUtils.assertTxnsMatch;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 * Date: 6/30/14
 */
public class RegionTxnStoreTest {

		@Test
		public void testCanWriteAndReadNewTransactionInformation() throws Exception {
				HRegion region = getMockRegion();

				RegionTxnStore store = new RegionTxnStore(region);

				SparseTxn txn = new SparseTxn(1,1,-1,-1,-1,true,true,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,new ByteSlice());
				store.recordTransaction(txn);

				SparseTxn transaction = store.getTransaction(txn.getTxnId());
				assertTxnsMatch("Transactions do not match!",txn,transaction);
		}

		@Test
		public void testCanCommitATransaction() throws Exception {
				HRegion region = getMockRegion();

				RegionTxnStore store = new RegionTxnStore(region);

				SparseTxn txn = new SparseTxn(1,1,-1,-1,-1,true,true,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,new ByteSlice());
				store.recordTransaction(txn);

				//check that insertion works
				SparseTxn transaction = store.getTransaction(txn.getTxnId());
				assertTxnsMatch("Transactions do not match!",txn,transaction);

				Txn.State currentState = store.getState(txn.getTxnId());
				Assert.assertEquals("Incorrect current state!",Txn.State.ACTIVE,currentState);

				long commitTs = 2l;
				store.recordCommit(txn.getTxnId(),commitTs);

				currentState = store.getState(txn.getTxnId());
				Assert.assertEquals("Incorrect current state!",Txn.State.COMMITTED,currentState);

				SparseTxn correctTxn = new SparseTxn(txn.getTxnId(),txn.getBeginTimestamp(),txn.getParentTxnId(),
								commitTs,txn.getGlobalCommitTimestamp(),
								txn.hasDependentField(),txn.isDependent(),txn.hasAdditiveField(),txn.isAdditive(),txn.getIsolationLevel(),
								Txn.State.COMMITTED,new ByteSlice());

				assertTxnsMatch("Transaction does not match committed state!",correctTxn,store.getTransaction(txn.getTxnId()));

				long actualCommitTs = store.getCommitTimestamp(txn.getTxnId());
				Assert.assertEquals("Incorrect commit timestamp from getCommitTimestamp()",commitTs,actualCommitTs);
		}

		@Test
		public void testCanRollbackATransaction() throws Exception {
				HRegion region = getMockRegion();

				RegionTxnStore store = new RegionTxnStore(region);

				SparseTxn txn = new SparseTxn(1,1,-1,-1,-1,true,true,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,new ByteSlice());
				store.recordTransaction(txn);

				//check that insertion works
				SparseTxn transaction = store.getTransaction(txn.getTxnId());
				assertTxnsMatch("Transactions do not match!",txn,transaction);

				store.recordRollback(txn.getTxnId());

				SparseTxn correctTxn = new SparseTxn(txn.getTxnId(),txn.getBeginTimestamp(),txn.getParentTxnId(),
								txn.getCommitTimestamp(),txn.getGlobalCommitTimestamp(),
								txn.hasDependentField(),txn.isDependent(),txn.hasAdditiveField(),txn.isAdditive(),txn.getIsolationLevel(),
								Txn.State.ROLLEDBACK,new ByteSlice());

				assertTxnsMatch("Transaction does not match committed state!",correctTxn,store.getTransaction(txn.getTxnId()));
		}

		@Test
		public void testCanGetActiveTransactions() throws Exception {
				HRegion region = getMockRegion();

				RegionTxnStore store = new RegionTxnStore(region);

				SparseTxn txn = new SparseTxn(1,1,-1,-1,-1,true,true,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,new ByteSlice());
				store.recordTransaction(txn);

				long[] activeTxnIds = store.getActiveTxnIds(0, 2,null);
				Assert.assertEquals("Incorrect length!",1,activeTxnIds.length);
				Assert.assertArrayEquals("Incorrect listing!",new long[]{1},activeTxnIds);
		}

		@Test
		public void testGetActiveTransactionsFiltersOutRolledbackTxns() throws Exception {
				HRegion region = getMockRegion();

				RegionTxnStore store = new RegionTxnStore(region);

				SparseTxn txn = new SparseTxn(1,1,-1,-1,-1,true,true,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,new ByteSlice());
				store.recordTransaction(txn);

				Thread.sleep(100); //sleep for 100 ms to ensure that the System.currentTimeMillis() moves forward
				store.recordRollback(txn.getTxnId());

				long[] activeTxnIds = store.getActiveTxnIds(0, 2,null);
				Assert.assertEquals("Incorrect length!",0,activeTxnIds.length);
		}

		@Test
		public void testGetActiveTransactionsFiltersOutCommittedTxns() throws Exception {
				HRegion region = getMockRegion();

				RegionTxnStore store = new RegionTxnStore(region);

				SparseTxn txn = new SparseTxn(1,1,-1,-1,-1,true,true,true,true, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE,new ByteSlice());
				store.recordTransaction(txn);

				Thread.sleep(100); //sleep for 100 ms to ensure that the System.currentTimeMillis() moves forward
				store.recordCommit(txn.getTxnId(),2l);

				long[] activeTxnIds = store.getActiveTxnIds(0, 2,null);
				Assert.assertEquals("Incorrect length!",0,activeTxnIds.length);
		}


		protected HRegion getMockRegion() throws IOException {
				final Map<byte[],Set<KeyValue>> rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				HRegion fakeRegion = mock(HRegion.class);

				when(fakeRegion.get(any(Get.class))).thenAnswer(new Answer<Result>(){
						@Override
						public Result answer(InvocationOnMock invocationOnMock) throws Throwable {
								final Get get = (Get)invocationOnMock.getArguments()[0];
								Set<KeyValue> keyValues = rowMap.get(get.getRow());
								if(get.hasFamilies()){
										Set<KeyValue> filtered = Sets.filter(keyValues, new Predicate<KeyValue>() {
												@Override
												public boolean apply(@Nullable KeyValue input) {
														Map<byte[], NavigableSet<byte[]>> familyMap = get.getFamilyMap();
														if (!familyMap.containsKey(input.getFamily())) return false;
														NavigableSet<byte[]> qualifiers = familyMap.get(input.getFamily());
														return qualifiers.contains(input.getQualifier());
												}
										});
										List<KeyValue> kvs = Lists.newArrayList(filtered);
										return new Result(kvs);
								}else
										return new Result(Lists.newArrayList(keyValues));
						}
				});

				doAnswer(new Answer<Void>(){
						@Override
						public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
								Put put = (Put)invocationOnMock.getArguments()[0];
								Set<KeyValue> keyValues = rowMap.get(put.getRow());
								if(keyValues==null){
										keyValues = Sets.newTreeSet(new KeyValue.KVComparator());
										rowMap.put(put.getRow(), keyValues);
								}
								Map<byte[], List<KeyValue>> familyMap = put.getFamilyMap();
								for(List<KeyValue> kvs:familyMap.values()){
										for(KeyValue kv:kvs){
												if(kv.isLatestTimestamp())
														kv = new KeyValue(kv.getRow(),kv.getFamily(),kv.getQualifier(),System.currentTimeMillis(),kv.getValue());
												keyValues.add(kv);
										}
								}
								return null;
						}
				}).when(fakeRegion).put(any(Put.class));

				when(fakeRegion.getScanner(any(Scan.class))).thenAnswer(new Answer<RegionScanner>(){

						@Override
						public RegionScanner answer(InvocationOnMock invocationOnMock) throws Throwable {
								Scan scan = (Scan)invocationOnMock.getArguments()[0];
								return new IteratorRegionScanner(rowMap.values().iterator(),scan);
						}
				});

				return fakeRegion;
		}

		private static class IteratorRegionScanner implements RegionScanner{
				private final PeekingIterator<Set<KeyValue>> kvs;
				private final Scan scan;
				private final Filter filter;

				private IteratorRegionScanner(Iterator<Set<KeyValue>> kvs, Scan scan) {
						this.kvs = Iterators.peekingIterator(kvs);
						this.scan = scan;
						this.filter = scan.getFilter();
				}

				@Override public HRegionInfo getRegionInfo() { return mock(HRegionInfo.class); }

				@Override
				public boolean isFilterDone() {
						return filter!=null && filter.filterAllRemaining();
				}

				@SuppressWarnings("LoopStatementThatDoesntLoop")
				@Override
				public boolean reseek(byte[] row) throws IOException {
						if(!kvs.hasNext()) return false;
						while(kvs.hasNext()){
								Set<KeyValue> next = kvs.peek();
								if(next.size()<0){
										kvs.next();  //throw empty rows away
										continue;
								}
								for(KeyValue kv: next){
										if(Bytes.equals(kv.getBuffer(),kv.getRowOffset(),kv.getRowLength(),row,0, row.length))
												return true;
										else{
												kvs.next();
												break;
										}
								}
						}
						return false;
				}

				@Override public long getMvccReadPoint() { return 0; }

				@Override
				public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
						return next(result);
				}

				@Override
				public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException {
						return next(result);
				}

				@Override
				public boolean next(List<KeyValue> results) throws IOException {
						OUTER: while(kvs.hasNext()){
								Set<KeyValue> next = kvs.next();
								List<KeyValue> toAdd = Lists.newArrayListWithCapacity(next.size());
								for(KeyValue kv:next){
										if(!containedInScan(kv)) continue; //skip KVs we aren't interested in
										if(filter!=null){
												Filter.ReturnCode retCode = filter.filterKeyValue(kv);
												switch(retCode){
														case INCLUDE:
														case INCLUDE_AND_NEXT_COL:
																toAdd.add(kv);
																break;
														case SKIP:
														case NEXT_COL:
																break;
														case NEXT_ROW:
																continue OUTER;
														case SEEK_NEXT_USING_HINT:
																throw new UnsupportedOperationException("DON'T USE THIS");
												}
										}
								}
								if(filter!=null)
										filter.filterRow(toAdd);
								if(toAdd.size()>0){
										results.addAll(toAdd);
										return true;
								}
						}
						return false;
				}

				private boolean containedInScan(KeyValue kv) {
						byte[] family = kv.getFamily();
						Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
						if(!familyMap.containsKey(family)) return false;
						NavigableSet<byte[]> qualifiersToFetch = familyMap.get(family);
						return qualifiersToFetch.contains(kv.getQualifier());
				}

				@Override
				public boolean next(List<KeyValue> results, String metric) throws IOException {
						return next(results);
				}

				@Override
				public boolean next(List<KeyValue> result, int limit) throws IOException {
						return next(result);
				}

				@Override
				public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
						return next(result);
				}

				@Override
				public void close() throws IOException {
					//no-op
				}
		}
}
