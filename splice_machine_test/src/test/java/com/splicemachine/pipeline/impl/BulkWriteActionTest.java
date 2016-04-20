package com.splicemachine.pipeline.impl;

import com.splicemachine.collections.SingletonSortedSet;
import com.splicemachine.concurrent.TickingClock;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.pipeline.api.BulkWritesInvoker;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.writeconfiguration.DefaultWriteConfiguration;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.utils.Sleeper;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 * Date: 1/31/14
 */
public class BulkWriteActionTest {
    private static final KryoPool kp=new KryoPool(1);

	@Test
	public void testDoesNotWriteDataWhenGivenAnEmptyBulkWrite() throws Exception{
		byte[] table=Bytes.toBytes("1424");
		TxnView txn=new ActiveWriteTxn(1L,1L,Txn.ROOT_TRANSACTION,true,Txn.IsolationLevel.SNAPSHOT_ISOLATION);
		Collection<BulkWrite> bwList=new ArrayList<>();

		BulkWrites bw=new BulkWrites(bwList,txn);
		ActionStatusReporter asr=new ActionStatusReporter();
		final BulkWritesInvoker writer = new FailWriter();
		BulkWritesInvoker.Factory bwf=new BulkWritesInvoker.Factory(){
            @Override
            public BulkWritesInvoker newInstance(){
                return writer;
            }
        };

		WriteConfiguration config = new DefaultWriteConfiguration(new Monitor(0,0,10,10L,0));
		IncrementingClock clock = new IncrementingClock(0);
		BulkWriteAction bwa = new BulkWriteAction(table,
				bw,
                mock(RegionCache.class),
				config,
				asr,
                bwf,
                clock);

		bwa.call();

		Assert.assertEquals("Should not have waited!",0,clock.currentTimeMillis());
	}

	@Test
	public void testDoesNotWriteDataWhenGivenBulkWriteWithNoRecords() throws Exception{
		byte[] table=Bytes.toBytes("1424");
		TxnView txn=new ActiveWriteTxn(1L,1L,Txn.ROOT_TRANSACTION,true,Txn.IsolationLevel.SNAPSHOT_ISOLATION);
		Collection<BulkWrite> bwList=new ArrayList<>();
		bwList.add(new BulkWrite(Collections.<KVPair>emptyList(),"region1"));

		BulkWrites bw=new BulkWrites(bwList,txn);
		ActionStatusReporter asr=new ActionStatusReporter();
		final BulkWritesInvoker writer = new FailWriter();
		BulkWritesInvoker.Factory bwf=new BulkWritesInvoker.Factory(){
            @Override
            public BulkWritesInvoker newInstance(){
                return writer;
            }
        };

		WriteConfiguration config = new DefaultWriteConfiguration(new Monitor(0,0,10,10L,0));
		IncrementingClock clock = new IncrementingClock(0);
		BulkWriteAction bwa = new BulkWriteAction(table,
				bw,
                mock(RegionCache.class),
				config,
				asr,
				bwf,
				clock);

		bwa.call();

		Assert.assertEquals("Should not have waited!",0,clock.currentTimeMillis());
	}

	@Test
	public void testCorrectlyRetriesWhenOneRegionStops() throws Exception{
		byte[] table=Bytes.toBytes("1424");
		TxnView txn=new ActiveWriteTxn(1L,1L,Txn.ROOT_TRANSACTION,true,Txn.IsolationLevel.SNAPSHOT_ISOLATION);
		Collection<BulkWrite> bwList=new ArrayList<>(2);

        Collection<KVPair> mutations=addData(0,10);
        byte[] boundary =BytesUtil.unsignedCopyAndIncrement(mutations.iterator().next().getRowKey());
        HRegionInfo hri1 = new HRegionInfo(TableName.valueOf(table),HConstants.EMPTY_START_ROW,boundary);
        bwList.add(new BulkWrite(mutations,hri1.getEncodedName()));
        HRegionInfo hri2 = new HRegionInfo(TableName.valueOf(table),boundary,HConstants.EMPTY_END_ROW);
        bwList.add(new BulkWrite(addData(100,10),hri2.getEncodedName()));

		BulkWrites bw=new BulkWrites(bwList,txn);
		ActionStatusReporter asr=new ActionStatusReporter();
		final TestBulkWriter writer = new TestBulkWriter(hri1.getEncodedName(),hri2.getEncodedName());
		BulkWritesInvoker.Factory bwf=new BulkWritesInvoker.Factory(){
            @Override
            public BulkWritesInvoker newInstance(){
                return writer;
            }
        };

        IncrementingClock clock = new IncrementingClock(0);
        ServerName sn = ServerName.valueOf("testServer:123",clock.currentTimeMillis());

        RegionCache mockRc = mock(RegionCache.class);
        SingletonSortedSet<Pair<HRegionInfo,ServerName>> regions=new SingletonSortedSet<>(Pair.newPair(hri2,sn),new Comparator<Pair<HRegionInfo,ServerName>>(){
            @Override
            public int compare(Pair<HRegionInfo, ServerName> o1,Pair<HRegionInfo, ServerName> o2){
                return o1.getFirst().compareTo(o2.getFirst());
            }

        });
        when(mockRc.getRegions(any(byte[].class))).thenReturn(regions);

		WriteConfiguration config = new DefaultWriteConfiguration(new Monitor(0,0,10,10L,0));
		BulkWriteAction bwa = new BulkWriteAction(table,
				bw,
                mockRc,
				config,
				asr,
				bwf,
				clock);

		bwa.call();

		Assert.assertTrue("Waited for the incorrect amount of time!",clock.currentTimeMillis()<10*10);
		Assert.assertEquals("Incorrect number of calls!",2,writer.callCount);
		Collection<KVPair> allData = writer.data;
		Set<KVPair> deduped = new HashSet<>(allData);
		Assert.assertEquals("Duplicate rows were inserted!",allData.size(),deduped.size());

		for(BulkWrite write:bwList){
			Collection<KVPair> toWrite = write.getMutations();
			for(KVPair mutation:toWrite){
				Assert.assertTrue("Missing write!",allData.contains(mutation));
			}
		}
	}

	@Test
	public void testCorrectlyRetriesPartialResults() throws Exception{
		byte[] table=Bytes.toBytes("1424");
		TxnView txn=new ActiveWriteTxn(1L,1L,Txn.ROOT_TRANSACTION,true,Txn.IsolationLevel.SNAPSHOT_ISOLATION);
		Collection<BulkWrite> bwList=new ArrayList<>(2);
        Collection<KVPair> mutations=addData(0,10);
        byte[] boundary =BytesUtil.unsignedCopyAndIncrement(mutations.iterator().next().getRowKey());
        HRegionInfo hri1 = new HRegionInfo(TableName.valueOf(table),HConstants.EMPTY_START_ROW,boundary);
        bwList.add(new BulkWrite(mutations,hri1.getEncodedName()));
        HRegionInfo hri2 = new HRegionInfo(TableName.valueOf(table),boundary,HConstants.EMPTY_END_ROW);
		bwList.add(new BulkWrite(addData(100,10),hri2.getEncodedName()));

		BulkWrites bw=new BulkWrites(bwList,txn);
		ActionStatusReporter asr=new ActionStatusReporter();
		final PartialTestBulkWriter writer = new PartialTestBulkWriter(hri1.getEncodedName(),hri2.getEncodedName());
		BulkWritesInvoker.Factory bwf=new BulkWritesInvoker.Factory(){
            @Override
            public BulkWritesInvoker newInstance(){
                return writer;
            }
        };

        IncrementingClock clock = new IncrementingClock(0);
        ServerName sn = ServerName.valueOf("testServer:123",clock.currentTimeMillis());

        RegionCache mockRc = mock(RegionCache.class);
        SingletonSortedSet<Pair<HRegionInfo,ServerName>> regions=new SingletonSortedSet<>(Pair.newPair(hri2,sn),new Comparator<Pair<HRegionInfo,ServerName>>(){
            @Override
            public int compare(Pair<HRegionInfo, ServerName> o1,Pair<HRegionInfo, ServerName> o2){
                return o1.getFirst().compareTo(o2.getFirst());
            }

        });
        when(mockRc.getRegions(any(byte[].class))).thenReturn(regions);

		WriteConfiguration config = new DefaultWriteConfiguration(new Monitor(0,0,10,10L,0));
		BulkWriteAction bwa = new BulkWriteAction(table,
				bw,
                mockRc,
				config,
				asr,
				bwf,
				clock);

		bwa.call();

		Assert.assertTrue("Waited for the incorrect amount of time!",clock.currentTimeMillis()<10*10);
		Assert.assertEquals("Incorrect number of calls!",2,writer.callCount);
		Collection<KVPair> allData = writer.data;
		Set<KVPair> deduped = new HashSet<>(allData);
		Assert.assertEquals("Duplicate rows were inserted!",allData.size(),deduped.size());

		for(BulkWrite write:bwList){
			Collection<KVPair> toWrite = write.getMutations();
			for(KVPair mutation:toWrite){
				Assert.assertTrue("Missing write!",allData.contains(mutation));
			}
		}
	}

	@Test
	public void testCorrectlyRetriesWhenOneRegionStopsButReturnsResult() throws Exception{
		byte[] table=Bytes.toBytes("1424");
		TxnView txn=new ActiveWriteTxn(1L,1L,Txn.ROOT_TRANSACTION,true,Txn.IsolationLevel.SNAPSHOT_ISOLATION);
		Collection<BulkWrite> bwList=new ArrayList<>(2);
        Collection<KVPair> mutations=addData(0,10);
        byte[] boundary =BytesUtil.unsignedCopyAndIncrement(mutations.iterator().next().getRowKey());
        HRegionInfo hri1 = new HRegionInfo(TableName.valueOf(table),HConstants.EMPTY_START_ROW,boundary);
        bwList.add(new BulkWrite(mutations,hri1.getEncodedName()));
        HRegionInfo hri2 = new HRegionInfo(TableName.valueOf(table),boundary,HConstants.EMPTY_END_ROW);
        bwList.add(new BulkWrite(addData(100,10),hri2.getEncodedName()));
		BulkWrites bw=new BulkWrites(bwList,txn);
		ActionStatusReporter asr=new ActionStatusReporter();
		final TestBulkWriter writer = new TestBulkWriter(hri1.getEncodedName(),hri2.getEncodedName());
		BulkWritesInvoker.Factory bwf=new BulkWritesInvoker.Factory(){
            @Override
            public BulkWritesInvoker newInstance(){
                return writer;
            }
        };

        IncrementingClock clock = new IncrementingClock(0);
        ServerName sn = ServerName.valueOf("testServer:123",clock.currentTimeMillis());

        RegionCache mockRc = mock(RegionCache.class);
        SingletonSortedSet<Pair<HRegionInfo,ServerName>> regions=new SingletonSortedSet<>(Pair.newPair(hri2,sn),new Comparator<Pair<HRegionInfo,ServerName>>(){
            @Override
            public int compare(Pair<HRegionInfo, ServerName> o1,Pair<HRegionInfo, ServerName> o2){
                return o1.getFirst().compareTo(o2.getFirst());
            }

        });
        when(mockRc.getRegions(any(byte[].class))).thenReturn(regions);

		WriteConfiguration config = new DefaultWriteConfiguration(new Monitor(0,0,10,10L,0));
		BulkWriteAction bwa = new BulkWriteAction(table,
				bw,
                mockRc,
				config,
				asr,
				bwf,
				clock);

		bwa.call();

		Assert.assertTrue("Waited for the incorrect amount of time!",clock.currentTimeMillis()<10*10);
		Assert.assertEquals("Incorrect number of calls!",2,writer.callCount);
		Collection<KVPair> allData = writer.data;
		Set<KVPair> deduped = new HashSet<>(allData);
		Assert.assertEquals("Duplicate rows were inserted!",allData.size(),deduped.size());

		for(BulkWrite write:bwList){
			Collection<KVPair> toWrite = write.getMutations();
			for(KVPair mutation:toWrite){
				Assert.assertTrue("Missing write!",allData.contains(mutation));
			}
		}
	}

	/* ****************************************************************************************************************/
    /*private helper methods*/
	private Collection<KVPair> addData(int startPoint,int size) throws IOException{
		Collection<KVPair> data=new ArrayList<>(size);
		for(int i=startPoint;i<startPoint+size;i++){
			KVPair kvP=encode("ryan"+i,null,i);
			data.add(kvP);
		}
		return data;
	}

	private KVPair encode(String name,String job,int age) throws IOException{
		com.carrotsearch.hppc.BitSet setCols=new com.carrotsearch.hppc.BitSet(3);
		com.carrotsearch.hppc.BitSet scalarCols=new com.carrotsearch.hppc.BitSet(3);
		com.carrotsearch.hppc.BitSet empty=new com.carrotsearch.hppc.BitSet();
		if(job!=null)
			setCols.set(1);
		if(age>=0){
			setCols.set(2);
			scalarCols.set(2);
		}

		EntryEncoder ee=EntryEncoder.create(kp,2,setCols,scalarCols,empty,empty);
		MultiFieldEncoder entryEncoder=ee.getEntryEncoder();
		if(job!=null)
			entryEncoder.encodeNext(job);
		if(age>=0)
			entryEncoder.encodeNext(age);

		byte[] value=ee.encode();
		return new KVPair(Encoding.encode(name),value);
	}

	private static class FailWriter implements BulkWritesInvoker{
		@Override
		public BulkWritesResult invoke(BulkWrites write,boolean refreshCache) throws IOException{
			Assert.fail("Should not be called!");
			return null;
		}
	}

	private static class TestBulkWriter implements BulkWritesInvoker{
		int callCount=0;
		List<KVPair> data=new ArrayList<>(20);
        final String region1Id;
        final String region2Id;

        TestBulkWriter(String region1Id,String region2Id){
            this.region1Id=region1Id;
            this.region2Id=region2Id;
        }

        @Override
		public BulkWritesResult invoke(BulkWrites write,boolean refreshCache) throws IOException{
			Collection<BulkWrite> bulkWrites=write.getBulkWrites();
			Collection<BulkWriteResult> results=new ArrayList<>(bulkWrites.size());
			if(callCount==0){
				Assert.assertTrue("Not enough data was written!",bulkWrites.size()>=2);
				boolean foundR1=false;
				for(BulkWrite bw : bulkWrites){
					if(bw.getEncodedStringName().equals(region1Id)){
						foundR1=true;
						data.addAll(bw.getMutations());
						results.add(new BulkWriteResult(WriteResult.success()));
					}else{
						results.add(new BulkWriteResult(WriteResult.notServingRegion()));
					}
				}
				Assert.assertTrue("Did not find the first region!",foundR1);
				callCount++;
			}else if(callCount==1){
				Assert.assertEquals("Incorrect number of BulkWrites attempted retry!",1,bulkWrites.size());
				for(BulkWrite bw : bulkWrites){
					Assert.assertEquals("Data sent to incorrect region!",region2Id,bw.getEncodedStringName());
					data.addAll(bw.getMutations());
					results.add(new BulkWriteResult(WriteResult.success()));
				}
				callCount++;
			}else{
				Assert.fail("Retried too many times!");
			}
			return new BulkWritesResult(results);
		}
	}

	private static class PartialTestBulkWriter implements BulkWritesInvoker{
		int callCount=0;
		Set<KVPair> data=new HashSet<>();
        final String region1Id;
        final String region2Id;

        PartialTestBulkWriter(String region1Id,String region2Id){
            this.region1Id=region1Id;
            this.region2Id=region2Id;
        }

        @Override
        public BulkWritesResult invoke(BulkWrites write,boolean refreshCache) throws IOException{
            Collection<BulkWrite> bulkWrites=write.getBulkWrites();
            Collection<BulkWriteResult> results=new ArrayList<>(bulkWrites.size());
            if(callCount==0){
                Assert.assertTrue("Not enough data was written!",bulkWrites.size()>=2);
                for(BulkWrite bw : bulkWrites){
                    BulkWriteResult bwr = new BulkWriteResult(WriteResult.partial());
                    int i = 0;
                    for(KVPair kvp: bw.getMutations()){
                        if(i%2==0){
                            Assert.assertFalse("Row is already contained!",data.contains(kvp));
                            bwr.addResult(i,WriteResult.wrongRegion());
                        }else{
                            data.add(kvp);
                            bwr.addResult(i,WriteResult.success());
                        }
                        i++;
                    }
                    results.add(bwr);
                }
                callCount++;
            }else if(callCount==1){
                Assert.assertEquals("Incorrect number of BulkWrites attempted retry!",1,bulkWrites.size());
                for(BulkWrite bw : bulkWrites){
                    BulkWriteResult bwr = new BulkWriteResult(WriteResult.success());
                    Assert.assertEquals("Data sent to incorrect region!",region2Id,bw.getEncodedStringName());
                    int i=0;
                    for(KVPair kvp:bw.getMutations()){
                        Assert.assertFalse("Row was sent twice!",data.contains(kvp));
                        data.add(kvp);
                        bwr.addResult(i,WriteResult.success());
                    }
                    results.add(bwr);
                }
                callCount++;
            }else{
                Assert.fail("Retried too many times!");
            }
            return new BulkWritesResult(results);
        }
	}

    private static class IncrementingClock implements TickingClock,Sleeper{
        private int currentTime;

        IncrementingClock(int startNanoes){
            this.currentTime=startNanoes;
        }

        @Override
        public long tickMillis(long millis){
            this.currentTime+=TimeUnit.MILLISECONDS.toNanos(millis);
            return currentTime;
        }

        @Override
        public long tickNanos(long nanos){
            this.currentTime+=nanos;
            return currentTime;
        }

        //    @Override
        public void sleep(long time,TimeUnit unit) throws InterruptedException{
            tickNanos(unit.toNanos(time));
        }

        @Override
        public void sleep(long wait) throws InterruptedException{
            sleep(wait,TimeUnit.MILLISECONDS);
        }

        @Override
        public TimeView getSleepStats(){
            return Metrics.noOpTimeView();
        }

        @Override
        public long currentTimeMillis(){
            return TimeUnit.NANOSECONDS.toMillis(currentTime);
        }

        @Override
        public long nanoTime(){
            return currentTime;
        }
    }


	/*
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
/*
				final boolean[] splitPoint = new boolean[]{false,false};
				final RegionCache cache = mock(RegionCache.class);
				final Set<Pair<HRegionInfo,ServerName>> oneRegion = Sets.newTreeSet(new RegionCacheComparator());
				oneRegion.add(Pair.newPair(new HRegionInfo(Bytes.toBytes("test"),HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW),new ServerName(FOO_SERVERNAME)));
				final Set<Pair<HRegionInfo,ServerName>> twoRegions = Sets.newTreeSet(new RegionCacheComparator());
				twoRegions.add(Pair.newPair(new HRegionInfo(Bytes.toBytes("test"),HConstants.EMPTY_START_ROW,Encoding.encode(6)),new ServerName(FOO_SERVERNAME)));
				twoRegions.add(Pair.newPair(new HRegionInfo(Bytes.toBytes("test"),Encoding.encode(6),HConstants.EMPTY_END_ROW),new ServerName(FOO_SERVERNAME)));
				when(cache.getRegions(any(byte[].class))).thenAnswer(new Answer<Set<Pair<HRegionInfo,ServerName>>>() {
						@Override
						public Set<Pair<HRegionInfo,ServerName>> answer(InvocationOnMock invocation) throws Throwable {
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
				BulkWrite write = new BulkWrite(pairs,new ActiveWriteTxn(1l,1l),regionKey,"yo");
				WriteConfiguration config = mock(WriteConfiguration.class);
				when(config.getMaximumRetries()).thenReturn(5);
				when(config.getPause()).thenReturn(100l);
				when(config.globalError(any(Throwable.class))).thenReturn(WriteResponse.THROW_ERROR);
				when(config.partialFailure(any(BulkWriteResult.class), any(BulkWrite.class))).thenAnswer(new Answer<WriteResponse>() {
						@Override
						public WriteResponse answer(InvocationOnMock invocation) throws Throwable {
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
														return WriteResponse.THROW_ERROR;
										}
								}
								return WriteResponse.RETRY;
						}
				});

				final int[] state = new int[1]; //0 = NSR, 1 = WrongRegion, 2 = final attempt
				final BulkWritesInvoker invoker = mock(BulkWritesInvoker.class);
				final Map<byte[],Set<KVPair>> dataMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

				for(Pair<HRegionInfo,ServerName> info: twoRegions){
						dataMap.put(info.getFirst().getStartKey(),Sets.<KVPair>newHashSet());
				}

				when(invoker.invoke(any(BulkWrites.class),anyBoolean())).thenAnswer(new Answer<BulkWriteResult>() {
						@Override
						public BulkWriteResult answer(InvocationOnMock invocation) throws Throwable {
								BulkWrite write = (BulkWrite) invocation.getArguments()[0];
								if(state[0] == 0){
										IntObjectOpenHashMap<WriteResult> results = IntObjectOpenHashMap.newInstance();
										for(int i=0;i<write.numEntries();i++){
												results.put(i,WriteResult.notServingRegion());
										}
										state[0]++;
										return new BulkWriteResult(IntArrayList.newInstance(),results);
								}else {
										HRegionInfo region = null;
										for(Pair<HRegionInfo,ServerName> info:twoRegions){
												if(info.getFirst().containsRow(write.getRegionKey())){
														region = info.getFirst();
														break;
												}
										}
										Assert.assertNotNull("No Region found for start key!",region);
										Set<KVPair> pairSet = dataMap.get(region.getStartKey());

										IntObjectOpenHashMap<WriteResult> failedRows = IntObjectOpenHashMap.newInstance();
										Object[] kvBuffer = write.getBuffer();
										for(int i=0;i<write.numEntries();i++){
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

				BulkWriteAction action = new BulkWriteAction(Bytes.toBytes("test"),write,cache,config,new ActionStatusReporter(),new BulkWriteInvoker.Factory() {
						@Override
						public BulkWritesInvoker newInstance() {
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
				BulkWrite write = new BulkWrite(mutations,new ActiveWriteTxn(1l,1l),regionKey);
				RegionCache cache = mock(RegionCache.class);
				Writer.WriteConfiguration config = mock(Writer.WriteConfiguration.class);

				BulkWriteResult result = new BulkWriteResult(IntArrayList.newInstanceWithCapacity(0),
								IntObjectOpenHashMap.<WriteResult>newInstance(0,0.75f));
				BulkWriteInvoker invoker = mock(BulkWriteInvoker.class);
				when(invoker.invoke(write,false)).thenReturn(result);
				when(invoker.invoke(write,true)).thenReturn(result);
				BulkWriteInvoker.Factory factory = mock(BulkWriteInvoker.Factory.class);
				when(factory.newInstance()).thenReturn(invoker);

				ActionStatusReporter statusReporter = new ActionStatusReporter();
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
				BulkWrite write = new BulkWrite(mutations,new ActiveWriteTxn(1l,1l),regionKey);
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

				ActionStatusReporter statusReporter = new ActionStatusReporter();
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
				BulkWrite write = new BulkWrite(mutations,new ActiveWriteTxn(1l,1l),regionKey);
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

				ActionStatusReporter statusReporter = new ActionStatusReporter();
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
				BulkWrite write = new BulkWrite(mutations,new ActiveWriteTxn(1l,1l),regionKey);
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

				ActionStatusReporter statusReporter = new ActionStatusReporter();
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
				BulkWrite write = new BulkWrite(mutations,new ActiveWriteTxn(1l,1l),regionKey);
				RegionCache cache = mock(RegionCache.class);
				final SortedSet<Pair<HRegionInfo,ServerName>> regions = Sets.newTreeSet(new RegionCacheComparator());
				regions.add(Pair.newPair(new HRegionInfo(tableName, Encoding.encode(6),HConstants.EMPTY_END_ROW), new ServerName(FOO_SERVERNAME)));

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
				for(Pair<HRegionInfo,ServerName> info:regions){
						regionChecker.put(info.getFirst().getStartKey(),info.getFirst().getEndKey());
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
								for(int i=0;i<write.numEntries();i++){
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

				ActionStatusReporter statusReporter = new ActionStatusReporter();
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
				BulkWrite write = new BulkWrite(mutations,new ActiveWriteTxn(1l,1l),regionKey);
				RegionCache cache = mock(RegionCache.class);
				SortedSet<Pair<HRegionInfo,ServerName>> regions = Sets.newTreeSet(new RegionCacheComparator());
				regions.add(Pair.newPair(new HRegionInfo(tableName, HConstants.EMPTY_START_ROW,Encoding.encode(6)),new ServerName(FOO_SERVERNAME)));
				regions.add(Pair.newPair(new HRegionInfo(tableName, Encoding.encode(6),HConstants.EMPTY_END_ROW),new ServerName(FOO_SERVERNAME)));

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
				for(Pair<HRegionInfo,ServerName> info:regions){
						regionChecker.put(info.getFirst().getStartKey(),info.getFirst().getEndKey());
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
								for(int i=0;i<write.numEntries();i++){
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

				ActionStatusReporter statusReporter = new ActionStatusReporter();
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
				BulkWrite write = new BulkWrite(mutations,new ActiveWriteTxn(1l,1l),regionKey);
				RegionCache cache = mock(RegionCache.class);
				SortedSet<Pair<HRegionInfo,ServerName>> regions = Sets.newTreeSet(new RegionCacheComparator());
				regions.add(Pair.newPair(new HRegionInfo(tableName, HConstants.EMPTY_START_ROW,Encoding.encode(6)), new ServerName(FOO_SERVERNAME)));
				regions.add(Pair.newPair(new HRegionInfo(tableName, Encoding.encode(6),HConstants.EMPTY_END_ROW),new ServerName(FOO_SERVERNAME)));

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
				for(Pair<HRegionInfo,ServerName> info:regions){
						regionChecker.put(info.getFirst().getStartKey(),info.getFirst().getEndKey());
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
								for(int i=0;i<write.numEntries();i++){
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

				ActionStatusReporter statusReporter = new ActionStatusReporter();
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
				BulkWrite write = new BulkWrite(mutations,new ActiveWriteTxn(1l,1l),regionKey);
				RegionCache cache = mock(RegionCache.class);
				SortedSet<Pair<HRegionInfo,ServerName>> regions = Sets.newTreeSet(new RegionCacheComparator());
				regions.add(Pair.newPair(new HRegionInfo(tableName, HConstants.EMPTY_START_ROW,Encoding.encode(5)),new ServerName(FOO_SERVERNAME)));
				regions.add(Pair.newPair(new HRegionInfo(tableName, Encoding.encode(5),HConstants.EMPTY_END_ROW),new ServerName(FOO_SERVERNAME)));
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
				for(Pair<HRegionInfo,ServerName> info:regions){
						regionChecker.put(info.getFirst().getStartKey(),info.getFirst().getEndKey());
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
								for(int i=0;i<write.numEntries();i++){
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
				BulkWritesInvoker.Factory factory = mock(BulkWritesInvoker.Factory.class);
				when(factory.newInstance()).thenReturn(invoker);

				ActionStatusReporter statusReporter = new ActionStatusReporter();
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
*/
}
