package com.splicemachine.storage;

import com.google.common.collect.Lists;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.encoding.TestType;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Random;

import static org.mockito.Mockito.mock;

/**
 * @author Scott Fines
 *         Created on: 10/2/13
 */
@SuppressWarnings("StatementWithEmptyBody")
@RunWith(Parameterized.class)
public class SparseEntryAccumulatorTest {
    private static final KryoPool kryoPool = mock(KryoPool.class);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> data = Lists.newArrayList();

//        data.add(new Object[]{new TestType[]{TestType.SORTED_BYTES}});
        for(TestType type:TestType.values()){
            data.add(new Object[]{new TestType[]{type}});
        }
        for(TestType type:TestType.values()){
            for(TestType type2: TestType.values()){
                data.add(new Object[]{new TestType[]{type,type2}});
            }
        }
        return data;
    }

    private final TestType[] dataTypes;

    public SparseEntryAccumulatorTest(TestType[] dataTypes) {
        this.dataTypes = dataTypes;
    }

    @Test
    public void testMissingColumnsWorks() throws Exception {
        System.out.println(Arrays.toString(dataTypes));
        Random random = new Random(0l);
        int missingField = random.nextInt(dataTypes.length);
        BitSet fields  = new BitSet(dataTypes.length);
        fields.set(0,dataTypes.length);
        fields.clear(missingField);
        EntryAccumulator accumulator = new SparseEntryAccumulator(null,fields,true);

        Object[] correctData = new Object[dataTypes.length];
        MultiFieldEncoder encoder = MultiFieldEncoder.create(kryoPool,1);
        for(int i=0;i<dataTypes.length;i++){
            //skip the field that goes missing
            if(i==missingField)
                continue;

            encoder.reset();
            TestType type = dataTypes[i];
            Object correct = type.generateRandom(random);
            correctData[i] = correct;
            type.load(encoder,correct,false);
            accumulator.add(i, ByteBuffer.wrap(encoder.build()));
        }

        byte[] bytes = accumulator.finish();

        //get index offset
        int offset;
        for(offset=0;offset<bytes.length && bytes[offset]!=0;offset++);

        //make sure index is correct
        BitIndex index = BitIndexing.wrap(bytes,0,offset);
        for(int i=0;i<dataTypes.length;i++){
            if(i==missingField)
                Assert.assertFalse("Index contains entry "+i+" which should be missing",index.isSet(i));
            else
                Assert.assertTrue("Index missing entry " + i, index.isSet(i));
        }

        MultiFieldDecoder decoder =  MultiFieldDecoder.wrap(bytes,offset+1,bytes.length-offset-1,kryoPool);
        //make sure they all decode correctly
        for(int i=0;i<dataTypes.length;i++){
            if(i==missingField)continue;

            Object correct = correctData[i];
            TestType type = dataTypes[i];
            type.check(decoder,correct,false);
        }
    }

    @Test
    public void testCanAccumulateColumns() throws Exception {
//        System.out.println(Arrays.toString(dataTypes));
        Random random = new Random(0l);
        BitSet fields  = new BitSet(dataTypes.length);
        fields.set(0,dataTypes.length);
        EntryAccumulator accumulator = new SparseEntryAccumulator(null,fields,true);

        Object[] correctData = new Object[dataTypes.length];
        MultiFieldEncoder encoder = MultiFieldEncoder.create(kryoPool,1);
        for(int i=0;i<dataTypes.length;i++){
            encoder.reset();
            TestType type = dataTypes[i];
            Object correct = type.generateRandom(random);
            correctData[i] = correct;
            type.load(encoder,correct,false);
            accumulator.add(i, ByteBuffer.wrap(encoder.build()));
        }

        byte[] bytes = accumulator.finish();

        //get index offset
        int offset;
        for(offset=0;offset<bytes.length && bytes[offset]!=0;offset++);

        //make sure index is correct
        BitIndex index = BitIndexing.wrap(bytes,0,offset);
        for(int i=0;i<dataTypes.length;i++){
            Assert.assertTrue("Index missing entry "+ i,index.isSet(i));
        }

        MultiFieldDecoder decoder =  MultiFieldDecoder.wrap(bytes,offset+1,bytes.length-offset-1,kryoPool);
        //make sure they all decode correctly
        for(int i=0;i<dataTypes.length;i++){
            Object correct = correctData[i];
            TestType type = dataTypes[i];
            type.check(decoder,correct,false);
        }
    }
}
