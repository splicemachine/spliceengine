package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.batch.PipelineWriteContext;
import com.splicemachine.hbase.writer.*;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 * Created on: 9/25/13
 */
public class IndexUpsertWriteHandlerTest {

    @Test
    public void testDeleteFromIndexWorks() throws Exception {
        BitSet indexedColumns = new BitSet(1);
        indexedColumns.set(0);
        int[] mainColToIndexPos = new int[]{0};

        BitIndex index = BitIndexing.uncompressedBitMap(indexedColumns,new BitSet(),new BitSet(),new BitSet());
        EntryEncoder encoder = EntryEncoder.create(SpliceDriver.getKryoPool(), index);
        MultiFieldEncoder fieldEncoder = encoder.getEntryEncoder();
        final Set<KVPair> pairs = Sets.newTreeSet();
        for(int i=0;i<10;i++){
            fieldEncoder.reset();
            fieldEncoder.encodeNext(i);

            byte[] row = encoder.encode();

            KVPair next = new KVPair(Bytes.toBytes(i),row);
            pairs.add(next);

        }
        //add a null field to check
        fieldEncoder.reset();
        fieldEncoder.encodeEmpty();

        byte[] row = encoder.encode();

        KVPair next = new KVPair(Bytes.toBytes(11),row);
        pairs.add(next);

        List<KVPair> indexPairs = checkInsertionCorrect(indexedColumns, pairs);

        //get a delete write handler
        IndexDeleteWriteHandler deleteHandler = new IndexDeleteWriteHandler(
                indexedColumns,mainColToIndexPos,Bytes.toBytes("1184"),new BitSet(),true,6);

        //delete every other row in pairs
        Set<KVPair> deletedPairs = Sets.newTreeSet();
        boolean delete= true;
        for(KVPair pair:pairs){
            if(delete){
                deletedPairs.add(pair);
                delete=false;
            }else
                delete = true;
        }

        PipelineWriteContext context = getWriteContext(indexPairs);
        when(context.getTransactionId()).thenReturn(SpliceUtils.NA_TRANSACTION_ID);

        HRegion mockRegion = mock(HRegion.class);
        when(mockRegion.get(any(Get.class))).thenAnswer(new Answer<Result>(){

            @Override
            public Result answer(InvocationOnMock invocation) throws Throwable {
                Get get = (Get) invocation.getArguments()[0];

                byte[] rowKey = get.getRow();
                //get the KVPair on the main table with this row key
                for(KVPair pair:pairs){
                    if(Arrays.equals(pair.getRow(),rowKey)){
                        //convert to a Result object
                        KeyValue kv = pair.toKeyValue();
                        return new Result(Arrays.asList(kv));
                    }
                }
                return new Result();
            }
        });
        when(context.getRegion()).thenReturn(mockRegion);

        for(KVPair pairToDelete:deletedPairs){
            KVPair toDelete = new KVPair(pairToDelete.getRow(),pairToDelete.getValue(), KVPair.Type.DELETE);
            deleteHandler.updateIndex(toDelete,context);
        }

        //make sure that the index pairs size hasn't been changed until finish is called
        Assert.assertEquals("Incorrect row size before finish is called!",pairs.size(),indexPairs.size());

        deleteHandler.finish(context);

        Assert.assertEquals("Incorrect row size after finish is called!",pairs.size()-deletedPairs.size(),indexPairs.size());

        /*
         * Make sure none of the deleted rows are present.
         *
         * This is equivalent to checking that
         *
         * A) the main table and index table sizes are the same
         * B) the main table and index tables have the same rows present
         */
        Collection<KVPair> newMainTableRows = Sets.difference(pairs,deletedPairs);
        assertPresentInIndex(newMainTableRows,indexPairs);
    }

    @Test
    public void testInsertIntoIndexWorks() throws Exception {
        BitSet indexedColumns = new BitSet(1);
        indexedColumns.set(0);
        BitIndex index = BitIndexing.uncompressedBitMap(indexedColumns,new BitSet(),new BitSet(),new BitSet());
        EntryEncoder encoder = EntryEncoder.create(SpliceDriver.getKryoPool(), index);
        MultiFieldEncoder fieldEncoder = encoder.getEntryEncoder();
        Collection<KVPair> pairs = Sets.newTreeSet();
        for(int i=0;i<10;i++){
            fieldEncoder.reset();
            fieldEncoder.encodeNext(i);

            byte[] row = encoder.encode();

            KVPair next = new KVPair(Bytes.toBytes(i),row);
            pairs.add(next);

        }
        //add a null field to check
        fieldEncoder.reset();
        fieldEncoder.encodeEmpty();

        byte[] row = encoder.encode();

        KVPair next = new KVPair(Bytes.toBytes(11),row);
        pairs.add(next);

        checkInsertionCorrect(indexedColumns, pairs);
    }

    private List<KVPair> checkInsertionCorrect(BitSet indexedColumns, Collection<KVPair> pairs) throws Exception {
        final List<KVPair> indexPairs = Lists.newArrayList();
        PipelineWriteContext testCtx = getWriteContext(indexPairs);

        int[] mainColToIndexPos = new int[]{0};
        IndexUpsertWriteHandler writeHandler = getIndexUpsertWriteHandler(indexedColumns, mainColToIndexPos);

        for(KVPair pair:pairs){
            writeHandler.updateIndex(pair,testCtx);
        }

        //make sure nothing has been written yet
        Assert.assertEquals("Rows are written before being finalized!", 0, indexPairs.size());

        //finalize
        writeHandler.finish(testCtx);

        //make sure everything got written through
        Assert.assertEquals("Incorrect number of rows have been written!",pairs.size(),indexPairs.size());
        assertPresentInIndex(pairs, indexPairs);


        return indexPairs;
    }

    private void assertPresentInIndex(Collection<KVPair> pairs, List<KVPair> indexPairs) throws IOException {
        //make sure that every main row is found by doing a lookup on every index row
        EntryDecoder decoder = new EntryDecoder(SpliceDriver.getKryoPool());
        for(KVPair indexPair:indexPairs){
            decoder.set(indexPair.getValue());
            MultiFieldDecoder entryDecoder = decoder.getEntryDecoder();
            decoder.seekForward(entryDecoder,0);//skip data, go to byte[]
            ByteBuffer byteBuffer = decoder.nextAsBuffer(entryDecoder, 1);
            byte[] bits = new byte[byteBuffer.remaining()];
            byteBuffer.get(bits);
            byte[] rowPos = Encoding.decodeBytesUnsortd(bits, 0, bits.length);
            KVPair mainPair = new KVPair(rowPos,new byte[]{});
            Assert.assertTrue("Incorrect main table lookup!", pairs.contains(mainPair));
        }
    }

    private PipelineWriteContext getWriteContext(final List<KVPair> indexPairs) throws Exception {
        PipelineWriteContext testCtx = mock(PipelineWriteContext.class);
        doCallRealMethod().when(testCtx).sendUpstream(any(KVPair.class));
        when(testCtx.finish()).thenCallRealMethod();


        BufferConfiguration bufferConfiguration = getConstantBufferConfiguration();
        CallBuffer<KVPair> writingBuffer = new UnsafeCallBuffer<KVPair>(bufferConfiguration,new CallBuffer.Listener<KVPair>() {
            @Override
            public long heapSize(KVPair element) {
                return element.getSize();
            }

            @Override
            public void bufferFlushed(ObjectArrayList<KVPair> entries, CallBuffer<KVPair> source) throws Exception {
            	Object[] buffer = entries.buffer;
            	int size = entries.size();
            	for (int i = 0; i<size; i++) {
            		KVPair pair = (KVPair) buffer[i];
                    if(pair.getType()== KVPair.Type.DELETE){
                        Iterator<KVPair> itereator = indexPairs.iterator();
                        while(itereator.hasNext()){
                            KVPair existingPair = itereator.next();
                            if(Arrays.equals(pair.getRow(),existingPair.getRow())){
                                itereator.remove();
                                break;
                            }
                        }
                    }else{
                        indexPairs.add(pair);
                    }
                }
            }
        });

        when(testCtx.getWriteBuffer(
                any(byte[].class),
                any(WriteCoordinator.PreFlushHook.class),
                any(Writer.WriteConfiguration.class),any(int.class))).thenReturn(writingBuffer);
        return testCtx;
    }

    private IndexUpsertWriteHandler getIndexUpsertWriteHandler(BitSet indexedColumns, int[] mainColToIndexPos) {
        BitSet descColumns = new BitSet(1);
        boolean keepState = true;
        boolean unique = false;
        int expectedWrites = 10;
        byte[] indexConglomBytes = Bytes.toBytes("1184");

        IndexUpsertWriteHandler writeHandler = new IndexUpsertWriteHandler(indexedColumns,
                mainColToIndexPos,
                indexConglomBytes,
                descColumns,
                keepState,unique,
                expectedWrites);

        return writeHandler;
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
