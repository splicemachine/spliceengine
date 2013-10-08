package com.splicemachine.derby.impl.sql.execute.index;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.batch.PipelineWriteContext;
import com.splicemachine.hbase.writer.*;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.Snowflake;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 * Created on: 9/25/13
 */
public class IndexUpsertWriteHandlerTest {

    @Test
    public void testUpdatingIndexWorks() throws Exception {
        PipelineWriteContext testCtx = mock(PipelineWriteContext.class);
        doCallRealMethod().when(testCtx).sendUpstream(any(KVPair.class));
        when(testCtx.finish()).thenCallRealMethod();

        final List<KVPair> indexPairs = Lists.newArrayList();

        BufferConfiguration bufferConfiguration = getConstantBufferConfiguration();
        CallBuffer<KVPair> writingBuffer = new UnsafeCallBuffer<KVPair>(bufferConfiguration,new CallBuffer.Listener<KVPair>() {
            @Override
            public long heapSize(KVPair element) {
                return element.getSize();
            }

            @Override
            public void bufferFlushed(List<KVPair> entries, CallBuffer<KVPair> source) throws Exception {
                indexPairs.addAll(entries);
            }
        });

        when(testCtx.getWriteBuffer(
                any(byte[].class),
                any(WriteCoordinator.PreFlushHook.class),
                any(Writer.WriteConfiguration.class),any(int.class))).thenReturn(writingBuffer);

        BitSet indexedColumns = new BitSet(1);
        indexedColumns.set(0);
        int[] mainColToIndexPos = new int[]{0};
        BitSet descColumns = new BitSet(1);
        boolean keepState = true;
        boolean unique = false;
        int expectedWrites = 10;
        byte[] indexConglomBytes = Bytes.toBytes("1184");

        Snowflake snowflake = new Snowflake((short)1);
        Snowflake.Generator generator = snowflake.newGenerator(100);

        IndexUpsertWriteHandler writeHandler = new IndexUpsertWriteHandler(indexedColumns,
                mainColToIndexPos,
                indexConglomBytes,
                descColumns,
                keepState,unique,
                expectedWrites);

        writeHandler.setGenerator(generator);

        BitIndex index = BitIndexing.uncompressedBitMap(indexedColumns,new BitSet(),new BitSet(),new BitSet());
        EntryEncoder encoder = EntryEncoder.create(SpliceDriver.getKryoPool(), index);
        MultiFieldEncoder fieldEncoder = encoder.getEntryEncoder();
        List<KVPair> pairs =  Lists.newArrayList();
        for(int i=0;i<10;i++){
            fieldEncoder.reset();
            fieldEncoder.encodeNext(i);

            byte[] row = encoder.encode();

            KVPair next = new KVPair(Bytes.toBytes(i),row);
            pairs.add(next);

            writeHandler.updateIndex(next,testCtx);
        }
        //add a null field to check
        fieldEncoder.reset();
        fieldEncoder.encodeEmpty();

        byte[] row = encoder.encode();

        KVPair next = new KVPair(Bytes.toBytes(11),row);
        pairs.add(next);

        writeHandler.updateIndex(next,testCtx);

        //make sure nothing has been written yet
        Assert.assertEquals("Rows are written before being finalized!",0,indexPairs.size());

        //finalize
        writeHandler.finish(testCtx);

        //make sure everything got written through
        Assert.assertEquals("Incorrect number of rows have been written!",pairs.size(),indexPairs.size());
    }

    private Writer.WriteConfiguration getConstantsWriteConfiguration() throws ExecutionException {
        Writer.WriteConfiguration config = mock(Writer.WriteConfiguration.class);
        when(config.getMaximumRetries()).thenReturn(SpliceConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
        when(config.getPause()).thenReturn(1000l);

        return config;
    }

    private BufferConfiguration getConstantBufferConfiguration() {
        return new BufferConfiguration() {
                @Override
                public long getMaxHeapSize() {
                    return 2 * 1024 * 1024;
                }

                @Override
                public int getMaxEntries() {
                    return 1000;
                }

                @Override
                public int getMaxFlushesPerRegion() {
                    return 5;
                }

                @Override
                public void writeRejected() {
                    /*no-op*/
                }
            };
    }
}
