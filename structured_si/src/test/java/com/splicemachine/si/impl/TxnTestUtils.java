package com.splicemachine.si.impl;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.ByteBufferArrayUtils;
import com.splicemachine.si.api.Txn;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class TxnTestUtils {
		private TxnTestUtils(){}

		public static void assertTxnsMatch(String baseErrorMessage, SparseTxn correct, SparseTxn actual) {
				Assert.assertEquals(baseErrorMessage + " TxnIds differ", correct.getTxnId(), actual.getTxnId());
				Assert.assertEquals(baseErrorMessage + " Parent txn ids differ", correct.getParentTxnId(), actual.getParentTxnId());
				Assert.assertEquals(baseErrorMessage + " Begin timestamps differ", correct.getBeginTimestamp(), actual.getBeginTimestamp());
				Assert.assertEquals(baseErrorMessage + " HasDependent property differs", correct.hasDependentField(), actual.hasDependentField());
				Assert.assertEquals(baseErrorMessage + " Dependent property differs", correct.isDependent(), actual.isDependent());
				Assert.assertEquals(baseErrorMessage + " HasAdditive property differs", correct.hasAdditiveField(), actual.hasAdditiveField());
				Assert.assertEquals(baseErrorMessage + " Additive property differs", correct.isAdditive(), actual.isAdditive());
				Assert.assertEquals(baseErrorMessage + " Isolation level differs", correct.getIsolationLevel(), actual.getIsolationLevel());
				Assert.assertEquals(baseErrorMessage + " State differs", correct.getState(), actual.getState());
				Assert.assertEquals(baseErrorMessage + " Commit timestamp differs", correct.getCommitTimestamp(), actual.getCommitTimestamp());
				Assert.assertEquals(baseErrorMessage + " Global Commit timestamp differs", correct.getGlobalCommitTimestamp(), actual.getGlobalCommitTimestamp());
				ByteSlice correctDestTable = correct.getDestinationTableBuffer();
				ByteSlice actualDestTable = actual.getDestinationTableBuffer();
				if(correctDestTable==null || correctDestTable.length()<=0){
						if(actualDestTable!=null)
								Assert.assertTrue(baseErrorMessage + " Destination table differs. Should be empty, but has length " + actualDestTable.length(), actualDestTable.length() <= 0);
						//if actualDestTable==null, then it matches
				}  else{
						Assert.assertEquals(baseErrorMessage + " Destination table differs", correct.getDestinationTableBuffer(), actual.getDestinationTableBuffer());
				}
		}

		public static void assertTxnsMatch(String baseErrorMessage, Txn correct, Txn actual) {
				if(correct==actual) return; //they are the same object
				Assert.assertEquals(baseErrorMessage + " TxnIds differ", correct.getTxnId(), actual.getTxnId());
				assertTxnsMatch(baseErrorMessage + " Parent txns differ: ", correct.getParentTransaction(), actual.getParentTransaction());
				Assert.assertEquals(baseErrorMessage + " Begin timestamps differ", correct.getBeginTimestamp(), actual.getBeginTimestamp());
				Assert.assertEquals(baseErrorMessage + " Dependent property differs", correct.isDependent(), actual.isDependent());
				Assert.assertEquals(baseErrorMessage + " Additive property differs", correct.isAdditive(), actual.isAdditive());
				Assert.assertEquals(baseErrorMessage + " Isolation level differs", correct.getIsolationLevel(), actual.getIsolationLevel());
				Assert.assertEquals(baseErrorMessage + " State differs", correct.getState(), actual.getState());
				Assert.assertEquals(baseErrorMessage + " Commit timestamp differs", correct.getCommitTimestamp(), actual.getCommitTimestamp());
				Assert.assertEquals(baseErrorMessage + " Global Commit timestamp differs", correct.getGlobalCommitTimestamp(), actual.getGlobalCommitTimestamp());
				List<byte[]> correctTables = Lists.newArrayList(correct.getDestinationTables());
				List<byte[]> actualTables = Lists.newArrayList(actual.getDestinationTables());

				Collections.sort(correctTables, Bytes.BYTES_COMPARATOR);
				Collections.sort(actualTables,Bytes.BYTES_COMPARATOR);

				Assert.assertEquals(baseErrorMessage+ " Incorrect destination table size!",correctTables.size(),actualTables.size());
				for(int i=0;i<correctTables.size();i++){
						byte[] correctBytes = correctTables.get(i);
						byte[] actualBytes = actualTables.get(i);
						Assert.assertArrayEquals(baseErrorMessage+" Incorrect destination table at position "+ i,correctBytes,actualBytes);
				}
		}

		public static DataStore getMockDataStore() {
				DataStore ds = mock(DataStore.class);
				when(ds.getKeyValueType(any(KeyValue.class))).thenAnswer(new Answer<KeyValueType>() {
						@Override
						public KeyValueType answer(InvocationOnMock invocationOnMock) throws Throwable {
								KeyValue arg = (KeyValue)invocationOnMock.getArguments()[0];
								if(Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES, arg.getQualifier()))
										return KeyValueType.COMMIT_TIMESTAMP;
								else if(Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,arg.getQualifier()))
										return KeyValueType.TOMBSTONE;
								else if(Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,arg.getQualifier()))
										return KeyValueType.ANTI_TOMBSTONE;
								else if(Bytes.equals(SpliceConstants.PACKED_COLUMN_BYTES,arg.getQualifier()))
										return KeyValueType.USER_DATA;
								else return KeyValueType.OTHER;
						}
				});
				return ds;
		}

		public static HRegion getMockRegion() throws IOException {
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

				Answer<Void> putAnswer = new Answer<Void>() {
						@Override
						public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
								Put put = (Put) invocationOnMock.getArguments()[0];
								Set<KeyValue> keyValues = rowMap.get(put.getRow());
								if (keyValues == null) {
										keyValues = Sets.newTreeSet(new KeyValue.KVComparator());
										rowMap.put(put.getRow(), keyValues);
								}
								Map<byte[], List<KeyValue>> familyMap = put.getFamilyMap();
								for (List<KeyValue> kvs : familyMap.values()) {
										for (KeyValue kv : kvs) {
												if (kv.isLatestTimestamp())
														kv = new KeyValue(kv.getRow(), kv.getFamily(), kv.getQualifier(), System.currentTimeMillis(), kv.getValue());
												keyValues.add(kv);
										}
								}
								return null;
						}
				};
				doAnswer(putAnswer).when(fakeRegion).put(any(Put.class));
				doAnswer(putAnswer).when(fakeRegion).put(any(Put.class),anyBoolean());

				Answer<Void> deleteAnswer = new Answer<Void>() {
						@Override
						public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
								Delete delete = (Delete)invocationOnMock.getArguments()[0];
								Set<KeyValue> keyValues = rowMap.get(delete.getRow());
								if(keyValues==null) return null; //nothing to do, it's already deleted

								long timestamp = delete.getTimeStamp();
								boolean isEmpty = delete.isEmpty();
								if(isEmpty){
										Iterator<KeyValue> iter = keyValues.iterator();
										while(iter.hasNext()){
												KeyValue kv = iter.next();
												if(kv.getTimestamp()==timestamp)
														iter.remove();
										}
								}else{
										Map<byte[], List<KeyValue>> deleteFamilyMap = delete.getFamilyMap();
										Iterator<KeyValue> iter = keyValues.iterator();
										while(iter.hasNext()){
												KeyValue kv = iter.next();
												if(!deleteFamilyMap.containsKey(kv.getFamily()))
														continue;
												List<KeyValue> toDelete = deleteFamilyMap.get(kv.getFamily());
												if(toDelete.size()>0){
														for(KeyValue toDeleteKv:toDelete){
																if(toDeleteKv.getQualifier().length<=0){
																		//delete everything
																		if(kv.getTimestamp()==toDeleteKv.getTimestamp()){
																				iter.remove();
																				break;
																		}
																}
																else if(Bytes.equals(kv.getQualifier(),toDeleteKv.getQualifier())){
																		if(kv.getTimestamp()==toDeleteKv.getTimestamp()){
																				iter.remove();
																				break;
																		}
																}
														}
												}else{
														if(kv.getTimestamp()==timestamp)
																iter.remove();
												}
										}
								}


								return null;
						}
				};
				doAnswer(deleteAnswer).when(fakeRegion).delete(any(Delete.class),anyBoolean());

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
