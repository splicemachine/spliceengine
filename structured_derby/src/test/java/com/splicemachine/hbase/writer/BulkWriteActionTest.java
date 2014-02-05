package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.RegionCache;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.Sleeper;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

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

				final BulkWriteResult result = new BulkWriteResult(IntArrayList.newInstanceWithCapacity(0),
								IntObjectOpenHashMap.<WriteResult>newInstance(0,0.75f));
				BulkWriteInvoker invoker = mock(BulkWriteInvoker.class);
				final AtomicBoolean throwError = new AtomicBoolean(true);
				Answer<BulkWriteResult> answer = new Answer<BulkWriteResult>() {
						@Override
						public BulkWriteResult answer(InvocationOnMock invocationOnMock) throws Throwable {
								if (throwError.get())
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
								Assert.assertTrue("Slept more than once!",throwError.compareAndSet(true, false));
						}

						@Override public TimeView getSleepStats() { return Metrics.noOpTimeView(); }
				});
				action.call();
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
						@Override public long getPause() { return 10; }
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
