package com.splicemachine.derby.impl.sql.execute.index;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.uuid.Snowflake;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import com.splicemachine.concurrent.ResettableCountDownLatch;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.MockRegion;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.hbase.RegionCacheComparator;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.IndexWriteBufferFactory;
import com.splicemachine.pipeline.writehandler.IndexUpsertWriteHandler;
import com.splicemachine.pipeline.writehandler.RegionWriteHandler;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;

/**
 * @author Scott Fines
 * Created on: 9/25/13
 */
@Ignore
public class IndexedPipelineTest {
	public static final String FOO_SERVERNAME="example.org,1234,1212121212";

    @Test
    public void testClosingBeforeFinishWritesNoData() throws Exception {
        final ObjectArrayList<Mutation> mainTableWrites = ObjectArrayList.newInstance();
        HRegion testRegion = MockRegion.getMockRegion(MockRegion.getNotServingRegionAnswer());


				TxnSupplier supplier = mock(TxnSupplier.class);
				//TODO -sf- make this simpler
				TransactionalRegion txnRegion = new TxnRegion(testRegion, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
								supplier,mock(DataStore.class), mock(Transactor.class));
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        when(env.getRegion()).thenReturn(testRegion);
        final PipelineWriteContext testCtx = spy(new PipelineWriteContext(new IndexWriteBufferFactory(),new ActiveWriteTxn(1l,1l),txnRegion, env));

        //get a fake PipingWriteBuffer
        final TxnView txn = new ActiveWriteTxn(1l,1l);
        final ObjectArrayList<KVPair> indexedRows = ObjectArrayList.newInstance();
        final Writer fakeWriter = mockSuccessWriter(indexedRows);

        final RegionCache fakeCache = mockRegionCache();

        WriteConfiguration config = mock(WriteConfiguration.class);
				when(config.getMetricFactory()).thenReturn(Metrics.noOpMetricFactory());
        when(config.getMaximumRetries()).thenReturn(3);

        BitSet indexedColumns = new BitSet(1);
        indexedColumns.set(0);
        final IndexUpsertWriteHandler writeHandler = getIndexWriteHandler(indexedColumns);
        /*
        when(testCtx.getWriteBuffer(any(byte[].class), any(PreFlushHook.class), any(WriteConfiguration.class),any(int.class)))
                .thenAnswer(new Answer<PipingCallBuffer>() {
                    @Override
                    public PipingCallBuffer answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        byte[] indexName = (byte[]) args[0];
                        PreFlushHook preFlushHook = (PreFlushHook) args[1];
                        WriteConfiguration configuration = (WriteConfiguration) args[2];
                        int expectedSize = (Integer) args[3];
                        final BufferConfiguration bufferConfig = mock(BufferConfiguration.class);
                        when(bufferConfig.getMaxEntries()).thenReturn(expectedSize);
                        when(bufferConfig.getMaxHeapSize()).thenReturn(2 * 1024 * 1024l);
                        return new PipingCallBuffer(indexName, txn, fakeWriter, fakeCache, preFlushHook, configuration, bufferConfig);
                    }
                });
		*/

        BitIndex index = BitIndexing.uncompressedBitMap(indexedColumns, new BitSet(), new BitSet(), new BitSet());


        RegionWriteHandler regionHandler = new RegionWriteHandler(txnRegion,new ResettableCountDownLatch(0),100,null);
        testCtx.addLast(regionHandler);
        testCtx.addLast(writeHandler);

        EntryEncoder encoder = EntryEncoder.create(SpliceDriver.getKryoPool(), index);
        MultiFieldEncoder fieldEncoder = encoder.getEntryEncoder();
        ObjectArrayList<KVPair> mainTablePairs = ObjectArrayList.newInstance();
        for(int i=0;i<11;i++){
            fieldEncoder.reset();
            fieldEncoder.encodeNext(i);
            byte[] row = encoder.encode();

            KVPair next = new KVPair(Encoding.encode(i),row);
            mainTablePairs.add(next);
            testCtx.sendUpstream(next);
        }


        //make sure nothing has been written yet
        Assert.assertEquals("Writes have made it to the main table before finish has been called!",0,mainTableWrites.size());
        Assert.assertEquals("Writes have made it to the index table before finish has been called!",0,indexedRows.size());

        Map<KVPair, WriteResult> finishedResults = testCtx.close();

        //make sure nothing got through
        Assert.assertEquals("Incorrect number of writes have made it to the main table!",0,mainTableWrites.size());
        Assert.assertEquals("Incorrect number of writes have made it to the index table!", 0, indexedRows.size());

        //make sure everything reports NOT_SERVING_REGION
        for(WriteResult result:finishedResults.values()){
            Assert.assertEquals("Incorrect return code!", Code.NOT_SERVING_REGION, result.getCode());
        }
    }
    @Test
    public void testClosingInMiddleOfWritesWritesNoData() throws Exception {
        final ObjectArrayList<Mutation> mainTableWrites = ObjectArrayList.newInstance();
        HRegion testRegion = MockRegion.getMockRegion(MockRegion.getNotServingRegionAnswer());

				TxnSupplier supplier = mock(TxnSupplier.class);
				//TODO -sf- make this simpler
				TransactionalRegion txnRegion = new TxnRegion(testRegion, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
								supplier,mock(DataStore.class), mock(Transactor.class));
        final ActiveWriteTxn txn = new ActiveWriteTxn(1l, 1l);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        when(env.getRegion()).thenReturn(testRegion);
        final PipelineWriteContext testCtx = spy(new PipelineWriteContext(new IndexWriteBufferFactory(),txn,txnRegion, env));

        //get a fake PipingWriteBuffer
//        final String txnId = "1";
        final ObjectArrayList<KVPair> indexedRows = ObjectArrayList.newInstance();
        final Writer fakeWriter = mockSuccessWriter(indexedRows);

        final RegionCache fakeCache = mockRegionCache();

        WriteConfiguration config = mock(WriteConfiguration.class);
				when(config.getMetricFactory()).thenReturn(Metrics.noOpMetricFactory());
        when(config.getMaximumRetries()).thenReturn(3);

        BitSet indexedColumns = new BitSet(1);
        indexedColumns.set(0);
        final IndexUpsertWriteHandler writeHandler = getIndexWriteHandler(indexedColumns);
        /*
        when(testCtx.getWriteBuffer(any(byte[].class), any(PreFlushHook.class), any(WriteConfiguration.class),any(int.class)))
                .thenAnswer(new Answer<PipingCallBuffer>() {
                    @Override
                    public PipingCallBuffer answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        byte[] indexName = (byte[]) args[0];
                        PreFlushHook preFlushHook = (PreFlushHook) args[1];
                        WriteConfiguration configuration = (WriteConfiguration) args[2];
                        int expectedSize = (Integer) args[3];

                        final BufferConfiguration bufferConfig = mock(BufferConfiguration.class);
                        when(bufferConfig.getMaxEntries()).thenReturn(expectedSize);
                        when(bufferConfig.getMaxHeapSize()).thenReturn(2 * 1024 * 1024l);
                        return new PipingCallBuffer(indexName, txn, fakeWriter, fakeCache, preFlushHook, configuration, bufferConfig);
                    }
                });
		*/

        BitIndex index = BitIndexing.uncompressedBitMap(indexedColumns, new BitSet(), new BitSet(), new BitSet());


        RegionWriteHandler regionHandler = new RegionWriteHandler(txnRegion,new ResettableCountDownLatch(0),100,null);
        testCtx.addLast(regionHandler);
        testCtx.addLast(writeHandler);

        EntryEncoder encoder = EntryEncoder.create(SpliceDriver.getKryoPool(), index);
        MultiFieldEncoder fieldEncoder = encoder.getEntryEncoder();
        ObjectArrayList<KVPair> mainTablePairs = ObjectArrayList.newInstance();
        for(int i=0;i<10;i++){
            fieldEncoder.reset();
            fieldEncoder.encodeNext(i);
            byte[] row = encoder.encode();

            KVPair next = new KVPair(Encoding.encode(i),row);
            mainTablePairs.add(next);
            testCtx.sendUpstream(next);
        }

        //close the region
        when(testRegion.isClosing()).thenReturn(true);
        ObjectArrayList<KVPair> failedPairs = ObjectArrayList.newInstance();
        for(int i=10;i<20;i++){
            fieldEncoder.reset();
            fieldEncoder.encodeNext(i);
            byte[] row = encoder.encode();

            KVPair next = new KVPair(Encoding.encode(i),row);
            failedPairs.add(next);
            testCtx.sendUpstream(next);
        }

        //make sure nothing has been written yet
        Assert.assertEquals("Writes have made it to the main table before finish has been called!",0,mainTableWrites.size());
        Assert.assertEquals("Writes have made it to the index table before finish has been called!",0,indexedRows.size());

        Map<KVPair, WriteResult> finishedResults = testCtx.close();

        //make sure nothing got through
        Assert.assertEquals("Incorrect number of writes have made it to the main table!",0,mainTableWrites.size());
        Assert.assertEquals("Incorrect number of writes have made it to the index table!", 0, indexedRows.size());

        //make sure everything reports NOT_SERVING_REGION
        for(WriteResult result:finishedResults.values()){
            Assert.assertEquals("Incorrect return code!", Code.NOT_SERVING_REGION, result.getCode());
        }
    }


    @Test
    public void testWrongRegionRowsDoNotGetWritten() throws Exception {
        final ObjectArrayList<Mutation> mainTableWrites = ObjectArrayList.newInstance();
        HRegion testRegion = MockRegion.getMockRegion(MockRegion.getSuccessOnlyAnswer(mainTableWrites));
        when(testRegion.getRegionInfo().getEndKey()).thenReturn(Encoding.encode(10));

				TxnSupplier supplier = mock(TxnSupplier.class);
				//TODO -sf- make this simpler
				TransactionalRegion txnRegion = new TxnRegion(testRegion, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
								supplier,mock(DataStore.class), mock(Transactor.class));
        final String txnId = "1";
        final TxnView txn = new ActiveWriteTxn(1l,1l);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        when(env.getRegion()).thenReturn(testRegion);
        final PipelineWriteContext testCtx = spy(new PipelineWriteContext(new IndexWriteBufferFactory(),txn,txnRegion, env));

        //get a fake PipingWriteBuffer
        final ObjectArrayList<KVPair> indexedRows = ObjectArrayList.newInstance();
        final Writer fakeWriter = mockSuccessWriter(indexedRows);

        final RegionCache fakeCache = mockRegionCache();

        WriteConfiguration config = mock(WriteConfiguration.class);
				when(config.getMetricFactory()).thenReturn(Metrics.noOpMetricFactory());
        when(config.getMaximumRetries()).thenReturn(3);

        BitSet indexedColumns = new BitSet(1);
        indexedColumns.set(0);
        final IndexUpsertWriteHandler writeHandler = getIndexWriteHandler(indexedColumns);
        /*
        when(testCtx.getWriteBuffer(any(byte[].class), any(PreFlushHook.class), any(WriteConfiguration.class),any(int.class)))
                .thenAnswer(new Answer<PipingCallBuffer>() {
                    @Override
                    public PipingCallBuffer answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        byte[] indexName = (byte[]) args[0];
                        PreFlushHook preFlushHook = (PreFlushHook) args[1];
                        WriteConfiguration configuration = (WriteConfiguration) args[2];
                        int expectedSize = (Integer) args[3];

                        final BufferConfiguration bufferConfig = mock(BufferConfiguration.class);
                        when(bufferConfig.getMaxEntries()).thenReturn(expectedSize);
                        when(bufferConfig.getMaxHeapSize()).thenReturn(2 * 1024 * 1024l);
                        return new PipingCallBuffer(indexName, txn, fakeWriter, fakeCache, preFlushHook, configuration, bufferConfig);
                    }
                });
	*/

        BitIndex index = BitIndexing.uncompressedBitMap(indexedColumns, new BitSet(), new BitSet(), new BitSet());


        RegionWriteHandler regionHandler = new RegionWriteHandler(txnRegion,new ResettableCountDownLatch(0),100,null);
        testCtx.addLast(regionHandler);
        testCtx.addLast(writeHandler);

        EntryEncoder encoder = EntryEncoder.create(SpliceDriver.getKryoPool(), index);
        MultiFieldEncoder fieldEncoder = encoder.getEntryEncoder();
        ObjectArrayList<KVPair> mainTablePairs = ObjectArrayList.newInstance();
        for(int i=0;i<10;i++){
            fieldEncoder.reset();
            fieldEncoder.encodeNext(i);
            byte[] row = encoder.encode();

            KVPair next = new KVPair(Encoding.encode(i),row);
            mainTablePairs.add(next);
            testCtx.sendUpstream(next);
        }

        //close the region
        List<KVPair> failedPairs = Lists.newArrayList();
        for(int i=10;i<20;i++){
            fieldEncoder.reset();
            fieldEncoder.encodeNext(i);
            byte[] row = encoder.encode();

            KVPair next = new KVPair(Encoding.encode(i),row);
            failedPairs.add(next);
            testCtx.sendUpstream(next);
        }

        //make sure nothing has been written yet
        Assert.assertEquals("Writes have made it to the main table before finish has been called!",0,mainTableWrites.size());
        Assert.assertEquals("Writes have made it to the index table before finish has been called!",0,indexedRows.size());

        testCtx.flush();
        Map<KVPair, WriteResult> finishedResults = testCtx.close();

        //make sure nothing got through
        Assert.assertEquals("Incorrect number of writes have made it to the main table!",mainTablePairs.size(),mainTableWrites.size());
        Assert.assertEquals("Incorrect number of writes have made it to the index table!", mainTablePairs.size(), indexedRows.size());

        assertMainAndIndexRowsMatch(mainTableWrites,indexedRows,mainTablePairs,finishedResults,writeHandler.transformer);
        //make sure everything in failed reports WRONG_REGION
        for(KVPair pair:failedPairs){
            Assert.assertEquals("Incorrect status!", Code.WRONG_REGION,finishedResults.get(pair).getCode());
        }
    }

    @Test
    public void testClosingRegionBeforeWritingDoesNotWriteAnywhere() throws Exception {
        final ObjectArrayList<Mutation> mainTableWrites = ObjectArrayList.newInstance();
        HRegion testRegion = MockRegion.getMockRegion(MockRegion.getSuccessOnlyAnswer(mainTableWrites));
        when(testRegion.isClosed()).thenReturn(true);

				TxnSupplier supplier = mock(TxnSupplier.class);
				//TODO -sf- make this simpler
				TransactionalRegion txnRegion = new TxnRegion(testRegion, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
								supplier,mock(DataStore.class), mock(Transactor.class));
        final TxnView txn = new ActiveWriteTxn(1l,1l);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        when(env.getRegion()).thenReturn(testRegion);
        final PipelineWriteContext testCtx = spy(new PipelineWriteContext(new IndexWriteBufferFactory(),txn,txnRegion, env));

        //get a fake PipingWriteBuffer
        final ObjectArrayList<KVPair> indexedRows = ObjectArrayList.newInstance();
        final Writer fakeWriter = mockSuccessWriter(indexedRows);

        final RegionCache fakeCache = mockRegionCache();

        WriteConfiguration config = mock(WriteConfiguration.class);
				when(config.getMetricFactory()).thenReturn(Metrics.noOpMetricFactory());
        when(config.getMaximumRetries()).thenReturn(3);

        BitSet indexedColumns = new BitSet(1);
        indexedColumns.set(0);
        final IndexUpsertWriteHandler writeHandler = getIndexWriteHandler(indexedColumns);
        /*
        when(testCtx.getWriteBuffer(any(byte[].class), any(PreFlushHook.class), any(WriteConfiguration.class),any(int.class)))
                .thenAnswer(new Answer<PipingCallBuffer>() {
                    @Override
                    public PipingCallBuffer answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        byte[] indexName = (byte[]) args[0];
                        PreFlushHook preFlushHook = (PreFlushHook) args[1];
                        WriteConfiguration configuration = (WriteConfiguration) args[2];
                        int expectedSize = (Integer)args[3];

                        final BufferConfiguration bufferConfig = mock(BufferConfiguration.class);
                        when(bufferConfig.getMaxEntries()).thenReturn(expectedSize);
                        when(bufferConfig.getMaxHeapSize()).thenReturn(2*1024*1024l);

                        return new PipingCallBuffer(indexName, txn, fakeWriter, fakeCache, preFlushHook, configuration, bufferConfig);
                    }
                });
	*/

        BitIndex index = BitIndexing.uncompressedBitMap(indexedColumns, new BitSet(), new BitSet(), new BitSet());


        RegionWriteHandler regionHandler = new RegionWriteHandler(txnRegion,new ResettableCountDownLatch(0),100,null);
        testCtx.addLast(regionHandler);
        testCtx.addLast(writeHandler);

        ObjectArrayList<KVPair> mainTablePairs = ObjectArrayList.newInstance();
        EntryEncoder encoder = EntryEncoder.create(SpliceDriver.getKryoPool(), index);
        MultiFieldEncoder fieldEncoder = encoder.getEntryEncoder();
        for(int i=0;i<11;i++){
            fieldEncoder.reset();
            fieldEncoder.encodeNext(i);
            byte[] row = encoder.encode();

            KVPair next = new KVPair(Encoding.encode(i),row);
            mainTablePairs.add(next);
            testCtx.sendUpstream(next);
        }

        //make sure nothing has been written yet
        Assert.assertEquals("Writes have made it to the main table before finish has been called!",0,mainTableWrites.size());
        Assert.assertEquals("Writes have made it to the index table before finish has been called!",0,indexedRows.size());

        Map<KVPair, WriteResult> finishedResults = testCtx.close();

        //make sure nothing got through
        Assert.assertEquals("Incorrect number of writes have made it to the main table!",0,mainTableWrites.size());
        Assert.assertEquals("Incorrect number of writes have made it to the index table!", 0, indexedRows.size());

        //make sure everything reports NOT_SERVING_REGION
        for(WriteResult result:finishedResults.values()){
            Assert.assertEquals("Incorrect return code!", Code.NOT_SERVING_REGION,result.getCode());
        }
    }

    @Test
    public void testBulkWriteUpdatesBothIndexAndRegion() throws Exception {
        final ObjectArrayList<Mutation> mainTableWrites = ObjectArrayList.newInstance();
        HRegion testRegion = MockRegion.getMockRegion(MockRegion.getSuccessOnlyAnswer(mainTableWrites));

				TxnSupplier supplier = mock(TxnSupplier.class);
				//TODO -sf- make this simpler
				TransactionalRegion txnRegion = new TxnRegion(testRegion, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
								supplier,mock(DataStore.class), mock(Transactor.class));
        final TxnView txn = new ActiveWriteTxn(1l,1l);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        when(env.getRegion()).thenReturn(testRegion);
        PipelineWriteContext testCtx = spy(new PipelineWriteContext(new IndexWriteBufferFactory(),txn,txnRegion, env));

        //get a fake PipingWriteBuffer
        final ObjectArrayList<KVPair> indexedRows = ObjectArrayList.newInstance();
        final Writer fakeWriter = mockSuccessWriter(indexedRows);

        final RegionCache fakeCache = mockRegionCache();

        WriteConfiguration config = mock(WriteConfiguration.class);
				when(config.getMetricFactory()).thenReturn(Metrics.noOpMetricFactory());
        when(config.getMaximumRetries()).thenReturn(3);
/*
        when(testCtx.getWriteBuffer(any(byte[].class), any(PreFlushHook.class), notNull(WriteConfiguration.class),any(int.class)))
                .thenAnswer(new Answer<PipingCallBuffer>() {
                    @Override
                    public PipingCallBuffer answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        byte[] indexName = (byte[]) args[0];
                        PreFlushHook preFlushHook = (PreFlushHook) args[1];
                        WriteConfiguration configuration = (WriteConfiguration) args[2];
                        int expectedSize = (Integer) args[3];

                        final BufferConfiguration bufferConfig = mock(BufferConfiguration.class);
                        when(bufferConfig.getMaxEntries()).thenReturn(expectedSize + 10);
                        when(bufferConfig.getMaxHeapSize()).thenReturn(2 * 1024 * 1024l);
                        return new PipingCallBuffer(indexName, txn, fakeWriter, fakeCache, preFlushHook, configuration, bufferConfig);
                    }
                });
*/
        BitSet indexedColumns = new BitSet(1);
        indexedColumns.set(0);
        IndexUpsertWriteHandler writeHandler = getIndexWriteHandler(indexedColumns);

        BitIndex index = BitIndexing.uncompressedBitMap(indexedColumns, new BitSet(), new BitSet(), new BitSet());


        RegionWriteHandler regionHandler = new RegionWriteHandler(txnRegion,new ResettableCountDownLatch(0),100,null);
        testCtx.addLast(regionHandler);
        testCtx.addLast(writeHandler);

        ObjectArrayList<KVPair> mainTablePairs = ObjectArrayList.newInstance();
        EntryEncoder encoder = EntryEncoder.create(SpliceDriver.getKryoPool(), index);
        MultiFieldEncoder fieldEncoder = encoder.getEntryEncoder();
        for(int i=0;i<11;i++){
            fieldEncoder.reset();
            fieldEncoder.encodeNext(i);
            byte[] row = encoder.encode();

            KVPair next = new KVPair(Encoding.encode(i),row);
            mainTablePairs.add(next);
            testCtx.sendUpstream(next);
        }

        //make sure nothing has been written yet
        Assert.assertEquals("Writes have made it to the main table before finish has been called!",0,mainTableWrites.size());
        Assert.assertEquals("Writes have made it to the index table before finish has been called!",0,indexedRows.size());
        testCtx.flush();
        Map<KVPair, WriteResult> finishedResults = testCtx.close();

        //make sure the same number of rows are present in both cases
        Assert.assertEquals("Incorrect number of writes have made it to the main table!",mainTablePairs.size(),mainTableWrites.size());
        Assert.assertEquals("Incorrect number of writes have made it to the index table!", mainTablePairs.size(), indexedRows.size());
        /*
         * Need to check 3 things for every main table row:
         *
         * 1. result = SUCCESS
         * 2. row in main table
         * 3. transformed row in index
         *
         * 3 is hard to do--we basically have to compare the first N bytes of every row key, instead of
         * just doing a direct equality
         */
        IndexTransformer transformer = writeHandler.transformer;
        assertMainAndIndexRowsMatch(mainTableWrites, indexedRows, mainTablePairs, finishedResults, transformer);
    }

    private Writer mockSuccessWriter(final ObjectArrayList<KVPair> indexedRows) throws ExecutionException {
        Writer fakeWriter = mock(Writer.class);
        when(fakeWriter.write(any(byte[].class),any(BulkWrites.class),any(WriteConfiguration.class)))
                .then(new Answer<Future<WriteStats>>() {
                    @Override
                    public Future<WriteStats> answer(InvocationOnMock invocation) throws Throwable {
                        BulkWrites write = (BulkWrites) invocation.getArguments()[1];
                        indexedRows.addAll(write.getAllCombinedKeyValuePairs());

                        @SuppressWarnings("unchecked") Future<WriteStats> future = mock(Future.class);
                        when(future.get()).thenReturn(WriteStats.NOOP_WRITE_STATS);
                        return future;
                    }
                });
        return fakeWriter;
    }

    private RegionCache mockRegionCache() throws ExecutionException {
        SortedSet<Pair<HRegionInfo,ServerName>> indexRegionInfos = Sets.newTreeSet(new RegionCacheComparator());
        HRegionInfo indexRegionInfo = mock(HRegionInfo.class);
        when(indexRegionInfo.getStartKey()).thenReturn(HConstants.EMPTY_START_ROW);
        indexRegionInfos.add(Pair.newPair(indexRegionInfo, ServerName.valueOf(FOO_SERVERNAME)));
        RegionCache fakeCache = mock(RegionCache.class);
        when(fakeCache.getRegions(any(byte[].class))).thenReturn(indexRegionInfos);
        return fakeCache;
    }

    private IndexUpsertWriteHandler getIndexWriteHandler(BitSet indexedColumns) {
        int[] mainColToIndexPos = new int[]{0};
        BitSet descColumns = new BitSet(1);
        boolean keepState = true;
        boolean unique = false;
        boolean uniqueWithDuplicateNulls = false;
        int expectedWrites = 10;
        byte[] indexConglomBytes = Bytes.toBytes("1184");
        int[] format_ids = new int[]{80};

        Snowflake snowflake = new Snowflake((short)1);
        Snowflake.Generator generator = snowflake.newGenerator(100);

        IndexUpsertWriteHandler writeHandler = new IndexUpsertWriteHandler(indexedColumns,
                mainColToIndexPos,
                indexConglomBytes,
                descColumns,
                keepState,unique,
                uniqueWithDuplicateNulls,expectedWrites,null,format_ids);

        return writeHandler;
    }

    private void assertMainAndIndexRowsMatch(ObjectArrayList<Mutation> mainTableWrites,
    											ObjectArrayList<KVPair> indexedRows,
    											ObjectArrayList<KVPair> mainTablePairs,
                                             Map<KVPair, WriteResult> finishedResults,
                                             IndexTransformer transformer) throws IOException, StandardException {
    	Object[] pairBuffer = mainTablePairs.buffer;
    	for (int i = 0; i<mainTablePairs.size();i++) {
        	KVPair mainTablePair = (KVPair) pairBuffer[i];
            Assert.assertEquals("Incorrect result code!", Code.SUCCESS, finishedResults.get(mainTablePair).getCode());
            boolean found = false;
        	Object[] writeBuffer = mainTableWrites.buffer;
        	for (int j = 0; j<mainTableWrites.size();j++) {
        		Mutation finalMutation = (Mutation) writeBuffer[j];
                found = Bytes.equals(finalMutation.getRow(), mainTablePair.getRow());
                if(found)
                    break;
            }
            Assert.assertTrue("Row was not found in main table!", found);

            KVPair transformedRow = transformer.translate(mainTablePair);
            byte[] indexRow = transformedRow.getRow();
            int zeroIndex = 0;
            for(int k=0;k<indexRow.length;k++){
                if(indexRow[k]==0){
                    zeroIndex=k;
                    break;
                }
            }
            int numMatches = 0;
        	Object[] indexBuffer = indexedRows.buffer;
        	for (int l=0;l<indexedRows.size();l++) {
        		KVPair finalMutation = (KVPair) indexBuffer[l];
                byte[] mutationRow = finalMutation.getRow();
                found=true;
                for(int m=0;m<zeroIndex&&m<mutationRow.length;m++){
                    if(indexRow[m]!=mutationRow[m]){
                        found=false;
                        break;
                    }
                }
                if(found)
                    numMatches++;
            }
            Assert.assertEquals("Row was not found in index table!",1,numMatches);
        }
    }
}
