package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintContext;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.RegionCache;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.Sleeper;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 * Date: 1/31/14
 */
public class BulkWriteActionTest {

		@Test
		public void testCorrectWithSequence() throws Exception {
				/*
				 * The Sequence is
				 * 0. Begin with just one region []
				 * 1. Not ServingRegion (as code) for all rows
				 * 2. split into two regions ([], and b, for b such that some rows are extended past that)
				 * 3. some rows get WrongRegion when sent back to []
				 * 4. Ensure that they are properly split and sent to the correct region during retry
				 */
				final boolean[] splitPoint = new boolean[]{false,false};
				final RegionCache cache = mock(RegionCache.class);
				final Set<HRegionInfo> oneRegion = Sets.newTreeSet();
				oneRegion.add(new HRegionInfo(Bytes.toBytes("test"),HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW));
				final Set<HRegionInfo> twoRegions = Sets.newTreeSet();
				twoRegions.add(new HRegionInfo(Bytes.toBytes("test"),HConstants.EMPTY_START_ROW,Encoding.encode(6)));
				twoRegions.add(new HRegionInfo(Bytes.toBytes("test"),Encoding.encode(6),HConstants.EMPTY_END_ROW));
				when(cache.getRegions(any(byte[].class))).thenAnswer(new Answer<Set<HRegionInfo>>() {
						@Override
						public Set<HRegionInfo> answer(InvocationOnMock invocation) throws Throwable {
								if(splitPoint[0])
										return twoRegions;

								splitPoint[0] = true;
								return oneRegion;
						}
				});

				ObjectArrayList<KVPair> pairs = ObjectArrayList.newInstanceWithCapacity(10);
				for(int i=0;i<10;i++){
						pairs.add(new KVPair(Encoding.encode(i),Encoding.encode(i)));
				}
				byte[] regionKey = HConstants.EMPTY_START_ROW;
				BulkWrite write = new BulkWrite(pairs,"testTxnId",regionKey);
				Writer.WriteConfiguration config = mock(Writer.WriteConfiguration.class);
				when(config.getMaximumRetries()).thenReturn(5);
				when(config.getPause()).thenReturn(100l);
				when(config.globalError(any(Throwable.class))).thenReturn(Writer.WriteResponse.THROW_ERROR);
				when(config.partialFailure(any(BulkWriteResult.class), any(BulkWrite.class))).thenAnswer(new Answer<Writer.WriteResponse>() {
						@Override
						public Writer.WriteResponse answer(InvocationOnMock invocation) throws Throwable {
								BulkWriteResult result = (BulkWriteResult)invocation.getArguments()[0];
								for(IntObjectCursor<WriteResult> cursor:result.getFailedRows()){
										switch(cursor.value.getCode()){
												case FAILED:
												case WRITE_CONFLICT:
												case PRIMARY_KEY_VIOLATION:
												case UNIQUE_VIOLATION:
												case FOREIGN_KEY_VIOLATION:
												case CHECK_VIOLATION:
												case REGION_TOO_BUSY:
												case NOT_RUN:
												case SUCCESS:
														return Writer.WriteResponse.THROW_ERROR;
										}
								}
								return Writer.WriteResponse.RETRY;
						}
				});

				final int[] state = new int[1]; //0 = NSR, 1 = WrongRegion, 2 = final attempt
				final BulkWriteInvoker invoker = mock(BulkWriteInvoker.class);
				final Map<byte[],Set<KVPair>> dataMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

				for(HRegionInfo info: twoRegions){
						dataMap.put(info.getStartKey(),Sets.<KVPair>newHashSet());
				}

				when(invoker.invoke(any(BulkWrite.class),anyBoolean())).thenAnswer(new Answer<BulkWriteResult>() {
						@Override
						public BulkWriteResult answer(InvocationOnMock invocation) throws Throwable {
								BulkWrite write = (BulkWrite) invocation.getArguments()[0];
								if(state[0] == 0){
										IntObjectOpenHashMap<WriteResult> results = IntObjectOpenHashMap.newInstance();
										for(int i=0;i<write.getSize();i++){
												results.put(i,WriteResult.notServingRegion());
										}
										state[0]++;
										return new BulkWriteResult(IntArrayList.newInstance(),results);
								}else {
										HRegionInfo region = null;
										for(HRegionInfo info:twoRegions){
												if(info.containsRow(write.getRegionKey())){
														region = info;
														break;
												}
										}
										Assert.assertNotNull("No Region found for start key!",region);
										Set<KVPair> pairSet = dataMap.get(region.getStartKey());

										IntObjectOpenHashMap<WriteResult> failedRows = IntObjectOpenHashMap.newInstance();
										Object[] kvBuffer = write.getBuffer();
										for(int i=0;i<write.getSize();i++){
												KVPair pair = (KVPair)kvBuffer[i];
												if(!region.containsRow(pair.getRow())){
														failedRows.put(i,WriteResult.wrongRegion());
												}else{
														Assert.assertFalse("Already seen this row in this region!",pairSet.contains(pair));
														pairSet.add(pair);
												}
										}
										return new BulkWriteResult(IntArrayList.newInstance(),failedRows);
								}
						}
				});

				BulkWriteAction action = new BulkWriteAction(Bytes.toBytes("test"),write,cache,config,new BulkWriteAction.ActionStatusReporter(),new BulkWriteInvoker.Factory() {
						@Override
						public BulkWriteInvoker newInstance() {
								return invoker;
						}
				});

				action.call();
		}

		@Test
		public void testWorks() throws Exception {
				byte[] tableName = Bytes.toBytes("testTable");

				ObjectArrayList<KVPair> mutations = ObjectArrayList.newInstanceWithCapacity(5);
				for(int i=0;i<5;i++){
						mutations.add(new KVPair(Encoding.encode(i), Encoding.encode(i)));
				}
				byte[] regionKey = Encoding.encode(0);
				BulkWrite write = new BulkWrite(mutations,"testTxnId",regionKey);
				RegionCache cache = mock(RegionCache.class);
				Writer.WriteConfiguration config = mock(Writer.WriteConfiguration.class);

				BulkWriteResult result = new BulkWriteResult(IntArrayList.newInstanceWithCapacity(0),
								IntObjectOpenHashMap.<WriteResult>newInstance(0,0.75f));
				BulkWriteInvoker invoker = mock(BulkWriteInvoker.class);
				when(invoker.invoke(write,false)).thenReturn(result);
				when(invoker.invoke(write,true)).thenReturn(result);
				BulkWriteInvoker.Factory factory = mock(BulkWriteInvoker.Factory.class);
				when(factory.newInstance()).thenReturn(invoker);

				BulkWriteAction.ActionStatusReporter statusReporter = new BulkWriteAction.ActionStatusReporter();
				BulkWriteAction action = new BulkWriteAction(tableName,write,cache,config, statusReporter,factory);
				action.call();
		}

		@Test(expected= StandardException.class)
		public void testFailsOnNonRetryableFailure() throws Throwable {
				byte[] tableName = Bytes.toBytes("testTable");

				ObjectArrayList<KVPair> mutations = ObjectArrayList.newInstanceWithCapacity(5);
				for(int i=0;i<5;i++){
						mutations.add(new KVPair(Encoding.encode(i), Encoding.encode(i)));
				}
				byte[] regionKey = Encoding.encode(0);
				BulkWrite write = new BulkWrite(mutations,"testTxnId",regionKey);
				RegionCache cache = mock(RegionCache.class);
				Writer.WriteConfiguration config = mock(Writer.WriteConfiguration.class);
				when(config.partialFailure(any(BulkWriteResult.class),any(BulkWrite.class))).thenReturn(Writer.WriteResponse.THROW_ERROR);

				IntObjectOpenHashMap<WriteResult> failed = IntObjectOpenHashMap.newInstance();
				failed.put(0, new WriteResult(WriteResult.Code.UNIQUE_VIOLATION,new ConstraintContext("test","t_idx")));
				BulkWriteResult result = new BulkWriteResult(IntArrayList.newInstanceWithCapacity(0), failed);
				BulkWriteInvoker invoker = mock(BulkWriteInvoker.class);
				when(invoker.invoke(write,false)).thenReturn(result);
				when(invoker.invoke(write, true)).thenReturn(result);
				BulkWriteInvoker.Factory factory = mock(BulkWriteInvoker.Factory.class);
				when(factory.newInstance()).thenReturn(invoker);

				BulkWriteAction.ActionStatusReporter statusReporter = new BulkWriteAction.ActionStatusReporter();
				BulkWriteAction action = new BulkWriteAction(tableName,write,cache,config, statusReporter,factory);
				try{
						action.call();
						Assert.fail("No error thrown!");
				}catch(ExecutionException e){
						@SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable rootCause = Throwables.getRootCause(e);
						Assert.assertTrue(rootCause instanceof RetriesExhaustedWithDetailsException);
						throw Exceptions.parseException((RetriesExhaustedWithDetailsException)rootCause);
				}

		}

		@Test
		public void testRetriesOnRegionTooBusyCode() throws Exception {
				byte[] tableName = Bytes.toBytes("testTable");

				ObjectArrayList<KVPair> mutations = ObjectArrayList.newInstanceWithCapacity(5);
				for(int i=0;i<5;i++){
						mutations.add(new KVPair(Encoding.encode(i), Encoding.encode(i)));
				}
				byte[] regionKey = Encoding.encode(0);
				BulkWrite write = new BulkWrite(mutations,"testTxnId",regionKey);
				RegionCache cache = mock(RegionCache.class);
				Writer.WriteConfiguration config = mock(Writer.WriteConfiguration.class);
				when(config.getMaximumRetries()).thenReturn(1);

				final BulkWriteResult result = new BulkWriteResult(IntArrayList.newInstanceWithCapacity(0),
								IntObjectOpenHashMap.<WriteResult>newInstance(0,0.75f));
				BulkWriteInvoker invoker = mock(BulkWriteInvoker.class);
				final AtomicInteger errorCount = new AtomicInteger(3);
				Answer<BulkWriteResult> answer = new Answer<BulkWriteResult>() {
						@Override
						public BulkWriteResult answer(InvocationOnMock invocationOnMock) throws Throwable {
								if (errorCount.decrementAndGet()>0)
										return new BulkWriteResult(WriteResult.regionTooBusy());
								else
										return result;
						}
				};
				when(invoker.invoke(write,false)).thenAnswer(answer);
				when(invoker.invoke(write,true)).thenAnswer(answer);
				BulkWriteInvoker.Factory factory = mock(BulkWriteInvoker.Factory.class);
				when(factory.newInstance()).thenReturn(invoker);

				BulkWriteAction.ActionStatusReporter statusReporter = new BulkWriteAction.ActionStatusReporter();
				BulkWriteAction action = new BulkWriteAction(tableName,write,cache,config, statusReporter,factory, new Sleeper() {
						@Override
						public void sleep(long wait) throws InterruptedException {
								Assert.assertTrue("Slept after retries required!", errorCount.get()>0);
						}

						@Override public TimeView getSleepStats() { return Metrics.noOpTimeView(); }
				});
				action.call();
				Assert.assertEquals("Did not retry often enough!",0,errorCount.get());
		}

		@Test
		public void testRetriesOnRegionTooBusy() throws Exception {
				byte[] tableName = Bytes.toBytes("testTable");

				ObjectArrayList<KVPair> mutations = ObjectArrayList.newInstanceWithCapacity(5);
				for(int i=0;i<5;i++){
						mutations.add(new KVPair(Encoding.encode(i), Encoding.encode(i)));
				}
				byte[] regionKey = Encoding.encode(0);
				BulkWrite write = new BulkWrite(mutations,"testTxnId",regionKey);
				RegionCache cache = mock(RegionCache.class);
				Writer.WriteConfiguration config = mock(Writer.WriteConfiguration.class);
				when(config.getMaximumRetries()).thenReturn(1);

				final BulkWriteResult result = new BulkWriteResult(IntArrayList.newInstanceWithCapacity(0),
								IntObjectOpenHashMap.<WriteResult>newInstance(0,0.75f));
				BulkWriteInvoker invoker = mock(BulkWriteInvoker.class);
				final AtomicInteger errorCount = new AtomicInteger(3);
//				final AtomicBoolean throwError = new AtomicBoolean(true);
				Answer<BulkWriteResult> answer = new Answer<BulkWriteResult>() {
						@Override
						public BulkWriteResult answer(InvocationOnMock invocationOnMock) throws Throwable {
								if (errorCount.decrementAndGet()>0)
										throw new RegionTooBusyException("too many active ipc threads on region test");
								else
										return result;
						}
				};
				when(invoker.invoke(write,false)).thenAnswer(answer);
				when(invoker.invoke(write,true)).thenAnswer(answer);
				BulkWriteInvoker.Factory factory = mock(BulkWriteInvoker.Factory.class);
				when(factory.newInstance()).thenReturn(invoker);

				BulkWriteAction.ActionStatusReporter statusReporter = new BulkWriteAction.ActionStatusReporter();
				BulkWriteAction action = new BulkWriteAction(tableName,write,cache,config, statusReporter,factory, new Sleeper() {
						@Override
						public void sleep(long wait) throws InterruptedException {
								Assert.assertTrue("Slept after retries required!", errorCount.get()>0);
						}

						@Override public TimeView getSleepStats() { return Metrics.noOpTimeView(); }
				});
				action.call();
				Assert.assertEquals("Did not retry often enough!",0,errorCount.get());
		}

		@Test(expected = DoNotRetryIOException.class)
		public void testRetryFailsWhenCacheCantFill() throws Exception {
				byte[] tableName = Bytes.toBytes("testTable");
				ObjectArrayList<KVPair> mutations = ObjectArrayList.newInstanceWithCapacity(5);
				for(int i=0;i<10;i++){
						mutations.add(new KVPair(Encoding.encode(i), Encoding.encode(i)));
				}
				byte[] regionKey = Encoding.encode(5);
				BulkWrite write = new BulkWrite(mutations,"testTxnId",regionKey);
				RegionCache cache = mock(RegionCache.class);
				final SortedSet<HRegionInfo> regions = Sets.newTreeSet();
				regions.add(new HRegionInfo(tableName, Encoding.encode(6),HConstants.EMPTY_END_ROW));

				final boolean[] returnedRegions = new boolean[]{false};
				when(cache.getRegions(tableName)).thenAnswer(new Answer<Object>() {
						@Override
						public Object answer(InvocationOnMock invocation) throws Throwable {
								if(returnedRegions[0]){
										return null;
								}
								returnedRegions[0] = true;
								return regions;
						}
				});
				Writer.WriteConfiguration config = new Writer.WriteConfiguration() {
						@Override public int getMaximumRetries() { return 5; }
						@Override public long getPause() { return 1000; }
						@Override public void writeComplete(long timeTakenMs, long numRecordsWritten) {  }
						@Override public MetricFactory getMetricFactory() { return Metrics.noOpMetricFactory(); }

						@Override
						public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
								if(t instanceof NotServingRegionException)
										return Writer.WriteResponse.RETRY;
								return Writer.WriteResponse.THROW_ERROR;
						}

						@Override
						public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
								IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
								for(IntObjectCursor<WriteResult> failedRow:failedRows){
										if(failedRow.value.getCode()== WriteResult.Code.NOT_SERVING_REGION)
												return Writer.WriteResponse.RETRY;
								}
								return Writer.WriteResponse.THROW_ERROR;
						}

				};

				BulkWriteInvoker invoker = mock(BulkWriteInvoker.class);
				final NavigableMap<byte[],byte[]> regionChecker = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				for(HRegionInfo info:regions){
						regionChecker.put(info.getStartKey(),info.getEndKey());
				}
				final NavigableMap<byte[],Set<KVPair>> writtenRows = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				final AtomicBoolean regionSplit = new AtomicBoolean(false);
				Answer<BulkWriteResult> answer = new Answer<BulkWriteResult>() {
						@Override
						public BulkWriteResult answer(InvocationOnMock invocationOnMock) throws Throwable {
								BulkWrite write = (BulkWrite) invocationOnMock.getArguments()[0];
								byte[] startKey = write.getRegionKey();
								if(startKey.length>0 &&Encoding.decodeInt(startKey)==5){
										Assert.assertTrue("Attempted to send write to parent region twice!",regionSplit.compareAndSet(false,true));
										throw new NotServingRegionException("Region is closing");
								}
								byte[] regionEnd = regionChecker.get(startKey);
								Set<KVPair> kvPairs = writtenRows.get(startKey);
								if(kvPairs==null){
										kvPairs = Sets.newHashSet();
										writtenRows.put(startKey,kvPairs);
								}
								IntObjectOpenHashMap<WriteResult> results = IntObjectOpenHashMap.newInstance();
								Object[] buffer = write.getBuffer();
								for(int i=0;i<write.getSize();i++){
										KVPair pair = (KVPair) buffer[i];
										byte[] rowKey = pair.getRow();
										if(BytesUtil.startComparator.compare(startKey,rowKey)>0
														|| BytesUtil.endComparator.compare(regionEnd,rowKey)<=0){
												results.put(i, WriteResult.wrongRegion());
										}else{
												Assert.assertFalse("Row has already been seen!", kvPairs.contains(pair));
												kvPairs.add(pair);
										}
								}
								return new BulkWriteResult(IntArrayList.newInstanceWithCapacity(0),results);
						}
				};
				when(invoker.invoke(any(BulkWrite.class),anyBoolean())).thenAnswer(answer);
				BulkWriteInvoker.Factory factory = mock(BulkWriteInvoker.Factory.class);
				when(factory.newInstance()).thenReturn(invoker);

				BulkWriteAction.ActionStatusReporter statusReporter = new BulkWriteAction.ActionStatusReporter();
				BulkWriteAction action = new BulkWriteAction(tableName,write,cache,config, statusReporter,factory, mock(Sleeper.class));
				//should throw an error because the cache can't fill
				action.call();
		}

		@Test
		public void testRetriesCorrectlyOnRegionSplit() throws Exception {
				byte[] tableName = Bytes.toBytes("testTable");
				ObjectArrayList<KVPair> mutations = ObjectArrayList.newInstanceWithCapacity(5);
				for(int i=0;i<10;i++){
						mutations.add(new KVPair(Encoding.encode(i), Encoding.encode(i)));
				}
				byte[] regionKey = Encoding.encode(5);
				BulkWrite write = new BulkWrite(mutations,"testTxnId",regionKey);
				RegionCache cache = mock(RegionCache.class);
				SortedSet<HRegionInfo> regions = Sets.newTreeSet();
				regions.add(new HRegionInfo(tableName, HConstants.EMPTY_START_ROW,Encoding.encode(6)));
				regions.add(new HRegionInfo(tableName, Encoding.encode(6),HConstants.EMPTY_END_ROW));

				when(cache.getRegions(tableName)).thenReturn(regions);
				Writer.WriteConfiguration config = new Writer.WriteConfiguration() {
						@Override public int getMaximumRetries() { return 5; }
						@Override public long getPause() { return 1000; }
						@Override public void writeComplete(long timeTakenMs, long numRecordsWritten) {  }
						@Override public MetricFactory getMetricFactory() { return Metrics.noOpMetricFactory(); }

						@Override
						public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
								if(t instanceof NotServingRegionException)
										return Writer.WriteResponse.RETRY;
								return Writer.WriteResponse.THROW_ERROR;
						}

						@Override
						public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
								IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
								for(IntObjectCursor<WriteResult> failedRow:failedRows){
										if(failedRow.value.getCode()== WriteResult.Code.NOT_SERVING_REGION)
												return Writer.WriteResponse.RETRY;
								}
								return Writer.WriteResponse.THROW_ERROR;
						}

				};

				BulkWriteInvoker invoker = mock(BulkWriteInvoker.class);
				final AtomicBoolean throwError = new AtomicBoolean(true);
				final NavigableMap<byte[],byte[]> regionChecker = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				for(HRegionInfo info:regions){
						regionChecker.put(info.getStartKey(),info.getEndKey());
				}
				final NavigableMap<byte[],Set<KVPair>> writtenRows = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				final AtomicBoolean regionSplit = new AtomicBoolean(false);
				Answer<BulkWriteResult> answer = new Answer<BulkWriteResult>() {
						@Override
						public BulkWriteResult answer(InvocationOnMock invocationOnMock) throws Throwable {
								BulkWrite write = (BulkWrite) invocationOnMock.getArguments()[0];
								byte[] startKey = write.getRegionKey();
								if(startKey.length>0 &&Encoding.decodeInt(startKey)==5){
										Assert.assertTrue("Attempted to send write to parent region twice!",regionSplit.compareAndSet(false,true));
										throw new NotServingRegionException("Region is closing");
								}
								byte[] regionEnd = regionChecker.get(startKey);
								Set<KVPair> kvPairs = writtenRows.get(startKey);
								if(kvPairs==null){
										kvPairs = Sets.newHashSet();
										writtenRows.put(startKey,kvPairs);
								}
								IntObjectOpenHashMap<WriteResult> results = IntObjectOpenHashMap.newInstance();
								Object[] buffer = write.getBuffer();
								for(int i=0;i<write.getSize();i++){
										KVPair pair = (KVPair) buffer[i];
										byte[] rowKey = pair.getRow();
										if(BytesUtil.startComparator.compare(startKey,rowKey)>0
														|| BytesUtil.endComparator.compare(regionEnd,rowKey)<=0){
												results.put(i, WriteResult.wrongRegion());
										}else{
												Assert.assertFalse("Row has already been seen!", kvPairs.contains(pair));
												kvPairs.add(pair);
										}
								}
								return new BulkWriteResult(IntArrayList.newInstanceWithCapacity(0),results);
						}
				};
				when(invoker.invoke(any(BulkWrite.class),anyBoolean())).thenAnswer(answer);
				BulkWriteInvoker.Factory factory = mock(BulkWriteInvoker.Factory.class);
				when(factory.newInstance()).thenReturn(invoker);

				BulkWriteAction.ActionStatusReporter statusReporter = new BulkWriteAction.ActionStatusReporter();
				BulkWriteAction action = new BulkWriteAction(tableName,write,cache,config, statusReporter,factory, new Sleeper() {
						@Override
						public void sleep(long wait) throws InterruptedException {
								Assert.assertTrue("Slept more than once!",throwError.compareAndSet(true, false));
						}

						@Override public TimeView getSleepStats() { return Metrics.noOpTimeView(); }
				});
				action.call();
				Assert.assertFalse("did not sleep between retries",throwError.get());

				Assert.assertEquals("Rows were not written to both regions!",2,writtenRows.size());
				int totalRowsWritten=0;
				for(byte[] regionStartKey:writtenRows.keySet()){
						byte[] endKey = regionChecker.get(regionStartKey);
						Set<KVPair> rows = writtenRows.get(regionStartKey);
						for(KVPair row:rows){
								Assert.assertTrue("Row "+ row+" is not in the correct region!",BytesUtil.startComparator.compare(regionStartKey,row.getRow())<=0);
								Assert.assertTrue("Row "+ row+" is not in the correct region!",BytesUtil.endComparator.compare(endKey,row.getRow())>0);
								totalRowsWritten++;
						}
				}
				Assert.assertEquals("Incorrect number of rows written!",write.getMutations().size(),totalRowsWritten);
		}

		@Test
		public void testRetriesCorrectlyOnRegionSplitWithCodeAndNotRunInsteadOfError() throws Exception {
				byte[] tableName = Bytes.toBytes("testTable");
				ObjectArrayList<KVPair> mutations = ObjectArrayList.newInstanceWithCapacity(5);
				for(int i=0;i<10;i++){
						mutations.add(new KVPair(Encoding.encode(i), Encoding.encode(i)));
				}
				byte[] regionKey = Encoding.encode(5);
				BulkWrite write = new BulkWrite(mutations,"testTxnId",regionKey);
				RegionCache cache = mock(RegionCache.class);
				SortedSet<HRegionInfo> regions = Sets.newTreeSet();
				regions.add(new HRegionInfo(tableName, HConstants.EMPTY_START_ROW,Encoding.encode(6)));
				regions.add(new HRegionInfo(tableName, Encoding.encode(6),HConstants.EMPTY_END_ROW));

				when(cache.getRegions(tableName)).thenReturn(regions);
				Writer.WriteConfiguration config = new Writer.WriteConfiguration() {
						@Override public int getMaximumRetries() { return 5; }
						@Override public long getPause() { return 1000; }
						@Override public void writeComplete(long timeTakenMs, long numRecordsWritten) {  }
						@Override public MetricFactory getMetricFactory() { return Metrics.noOpMetricFactory(); }

						@Override
						public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
								if(t instanceof NotServingRegionException)
										return Writer.WriteResponse.RETRY;
								return Writer.WriteResponse.THROW_ERROR;
						}

						@Override
						public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
								IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
								for(IntObjectCursor<WriteResult> failedRow:failedRows){
										if(failedRow.value.getCode()== WriteResult.Code.NOT_SERVING_REGION)
												return Writer.WriteResponse.RETRY;
								}
								return Writer.WriteResponse.THROW_ERROR;
						}

				};

				BulkWriteInvoker invoker = mock(BulkWriteInvoker.class);
				final AtomicBoolean throwError = new AtomicBoolean(true);
				final NavigableMap<byte[],byte[]> regionChecker = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				for(HRegionInfo info:regions){
						regionChecker.put(info.getStartKey(),info.getEndKey());
				}
				final NavigableMap<byte[],Set<KVPair>> writtenRows = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				final AtomicBoolean regionSplit = new AtomicBoolean(false);
				Answer<BulkWriteResult> answer = new Answer<BulkWriteResult>() {
						@Override
						public BulkWriteResult answer(InvocationOnMock invocationOnMock) throws Throwable {
								BulkWrite write = (BulkWrite) invocationOnMock.getArguments()[0];
								byte[] startKey = write.getRegionKey();
								boolean isRegionSplit = false;
								if(startKey.length>0 &&Encoding.decodeInt(startKey)==5){
										Assert.assertTrue("Attempted to send write to parent region twice!", regionSplit.compareAndSet(false, true));
										isRegionSplit=true;
								}
								IntObjectOpenHashMap<WriteResult> failed = IntObjectOpenHashMap.newInstance();
								IntArrayList notRun = IntArrayList.newInstance();
								Object[] buffer = write.getBuffer();
								for(int i=0;i<write.getSize();i++){
										if(isRegionSplit){
												if(i==0)
														failed.put(i,WriteResult.notServingRegion());
												else{
														notRun.add(i);
												}
										} else{
												Set<KVPair> kvPairs = writtenRows.get(startKey);
												if(kvPairs==null){
														kvPairs = Sets.newHashSet();
														writtenRows.put(startKey,kvPairs);
												}
												KVPair pair = (KVPair) buffer[i];
//												byte[] rowKey = pair.getRow();
												Assert.assertFalse("Row has already been seen!", kvPairs.contains(pair));
												kvPairs.add(pair);
										}
								}
								return new BulkWriteResult(notRun,failed);
						}
				};
				when(invoker.invoke(any(BulkWrite.class),anyBoolean())).thenAnswer(answer);
				BulkWriteInvoker.Factory factory = mock(BulkWriteInvoker.Factory.class);
				when(factory.newInstance()).thenReturn(invoker);

				BulkWriteAction.ActionStatusReporter statusReporter = new BulkWriteAction.ActionStatusReporter();
				BulkWriteAction action = new BulkWriteAction(tableName,write,cache,config, statusReporter,factory, new Sleeper() {
						@Override
						public void sleep(long wait) throws InterruptedException {
								Assert.assertTrue("Slept more than once!",throwError.compareAndSet(true, false));
						}

						@Override public TimeView getSleepStats() { return Metrics.noOpTimeView(); }
				});
				action.call();
				Assert.assertFalse("did not sleep between retries",throwError.get());

				Assert.assertEquals("Rows were not written to both regions!",2,writtenRows.size());
				int totalRowsWritten=0;
				for(byte[] regionStartKey:writtenRows.keySet()){
						byte[] endKey = regionChecker.get(regionStartKey);
						Set<KVPair> rows = writtenRows.get(regionStartKey);
						for(KVPair row:rows){
								Assert.assertTrue("Row "+ row+" is not in the correct region!",BytesUtil.startComparator.compare(regionStartKey,row.getRow())<=0);
								Assert.assertTrue("Row "+ row+" is not in the correct region!",BytesUtil.endComparator.compare(endKey,row.getRow())>0);
								totalRowsWritten++;
						}
				}
				Assert.assertEquals("Incorrect number of rows written!",write.getMutations().size(),totalRowsWritten);
		}

		@Test
		public void testRetriesCorrectlyOnWrongRegion() throws Exception {
				byte[] tableName = Bytes.toBytes("testTable");

				ObjectArrayList<KVPair> mutations = ObjectArrayList.newInstanceWithCapacity(5);
				for(int i=0;i<10;i++){
						mutations.add(new KVPair(Encoding.encode(i), Encoding.encode(i)));
				}
				byte[] regionKey = Encoding.encode(5);
				BulkWrite write = new BulkWrite(mutations,"testTxnId",regionKey);
				RegionCache cache = mock(RegionCache.class);
				SortedSet<HRegionInfo> regions = Sets.newTreeSet();
				regions.add(new HRegionInfo(tableName, HConstants.EMPTY_START_ROW,Encoding.encode(5)));
				regions.add(new HRegionInfo(tableName, Encoding.encode(5),HConstants.EMPTY_END_ROW));
				when(cache.getRegions(tableName)).thenReturn(regions);
				Writer.WriteConfiguration config = new Writer.WriteConfiguration() {
						@Override public int getMaximumRetries() { return 5; }
						@Override public long getPause() { return 1000; }
						@Override public void writeComplete(long timeTakenMs, long numRecordsWritten) {  }
						@Override public MetricFactory getMetricFactory() { return Metrics.noOpMetricFactory(); }

						@Override
						public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
								if(t instanceof WrongRegionException)
										return Writer.WriteResponse.RETRY;
								return Writer.WriteResponse.THROW_ERROR;
						}

						@Override
						public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
								IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
								for(IntObjectCursor<WriteResult> failedRow:failedRows){
										if(failedRow.value.getCode()== WriteResult.Code.WRONG_REGION)
												return Writer.WriteResponse.RETRY;
								}
								return Writer.WriteResponse.RETRY;
						}

				};

				BulkWriteInvoker invoker = mock(BulkWriteInvoker.class);
				final AtomicBoolean throwError = new AtomicBoolean(true);
				final NavigableMap<byte[],byte[]> regionChecker = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				for(HRegionInfo info:regions){
						regionChecker.put(info.getStartKey(),info.getEndKey());
				}
				final NavigableMap<byte[],Set<KVPair>> writtenRows = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				Answer<BulkWriteResult> answer = new Answer<BulkWriteResult>() {
						@Override
						public BulkWriteResult answer(InvocationOnMock invocationOnMock) throws Throwable {
								BulkWrite write = (BulkWrite) invocationOnMock.getArguments()[0];
								byte[] startKey = write.getRegionKey();
								byte[] regionEnd = regionChecker.get(startKey);
								Set<KVPair> kvPairs = writtenRows.get(startKey);
								if(kvPairs==null){
										kvPairs = Sets.newHashSet();
										writtenRows.put(startKey,kvPairs);
								}
								IntObjectOpenHashMap<WriteResult> results = IntObjectOpenHashMap.newInstance();
								Object[] buffer = write.getBuffer();
								for(int i=0;i<write.getSize();i++){
										KVPair pair = (KVPair) buffer[i];
										byte[] rowKey = pair.getRow();
										if(BytesUtil.startComparator.compare(startKey,rowKey)>0
														|| BytesUtil.endComparator.compare(regionEnd,rowKey)<=0){
												results.put(i, WriteResult.wrongRegion());
										}else{
												Assert.assertFalse("Row has already been seen!", kvPairs.contains(pair));
												kvPairs.add(pair);
										}
								}
								return new BulkWriteResult(IntArrayList.newInstanceWithCapacity(0),results);
						}
				};
				when(invoker.invoke(any(BulkWrite.class),anyBoolean())).thenAnswer(answer);
				BulkWriteInvoker.Factory factory = mock(BulkWriteInvoker.Factory.class);
				when(factory.newInstance()).thenReturn(invoker);

				BulkWriteAction.ActionStatusReporter statusReporter = new BulkWriteAction.ActionStatusReporter();
				BulkWriteAction action = new BulkWriteAction(tableName,write,cache,config, statusReporter,factory, new Sleeper() {
						@Override
						public void sleep(long wait) throws InterruptedException {
								Assert.assertTrue("Slept more than once!",throwError.compareAndSet(true, false));
						}

						@Override public TimeView getSleepStats() { return Metrics.noOpTimeView(); }
				});
				action.call();
				Assert.assertFalse("did not sleep between retries",throwError.get());

				Assert.assertEquals("Rows were not written to both regions!",2,writtenRows.size());
				int totalRowsWritten=0;
				for(byte[] regionStartKey:writtenRows.keySet()){
						byte[] endKey = regionChecker.get(regionStartKey);
						Set<KVPair> rows = writtenRows.get(regionStartKey);
						for(KVPair row:rows){
								Assert.assertTrue("Row "+ row+" is not in the correct region!",BytesUtil.startComparator.compare(regionStartKey,row.getRow())<=0);
								Assert.assertTrue("Row "+ row+" is not in the correct region!",BytesUtil.endComparator.compare(endKey,row.getRow())>0);
								totalRowsWritten++;
						}
				}
				Assert.assertEquals("Incorrect number of rows written!",write.getMutations().size(),totalRowsWritten);
		}

}
