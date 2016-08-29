/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.storage;

import org.spark_project.guava.collect.Lists;
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

import java.util.Collection;
import java.util.Random;

import static org.mockito.Mockito.mock;

/**
 * @author Scott Fines
 *         Created on: 10/2/13
 */
@SuppressWarnings("StatementWithEmptyBody")
@RunWith(Parameterized.class)
public class AlwaysAcceptEntryAccumulatorTest {
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

    public AlwaysAcceptEntryAccumulatorTest(TestType[] dataTypes) {
        this.dataTypes = dataTypes;
    }

    @Test
    public void testMissingColumnsWorks() throws Exception {
//        System.out.println(Arrays.toString(dataTypes));
        Random random = new Random(0l);
        int missingField = random.nextInt(dataTypes.length);
        EntryAccumulator accumulator = new ByteEntryAccumulator(null,true);

        Object[] correctData = new Object[dataTypes.length];
        MultiFieldEncoder encoder = MultiFieldEncoder.create(1);
        for(int i=0;i<dataTypes.length;i++){
            //skip the field that goes missing
            if(i==missingField)
                continue;

            encoder.reset();
            TestType type = dataTypes[i];
            Object correct = type.generateRandom(random);
            correctData[i] = correct;
            type.load(encoder,correct,false);
            accumulate(accumulator, encoder, i, type);
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

        MultiFieldDecoder decoder =  MultiFieldDecoder.wrap(bytes,offset+1,bytes.length-offset-1);
        //make sure they all decode correctly
        for(int i=0;i<dataTypes.length;i++){
            if(i==missingField)continue;

            Object correct = correctData[i];
            TestType type = dataTypes[i];
            type.check(decoder,correct,false);
        }
    }

    private void accumulate(EntryAccumulator accumulator, MultiFieldEncoder encoder, int i, TestType type) {
				byte[] data = encoder.build();
				if(type.isScalarType())
            accumulator.addScalar(i,data,0,data.length);
        else if(type==TestType.FLOAT)
            accumulator.addFloat(i, data,0,data.length);
        else if(type==TestType.DOUBLE)
            accumulator.addDouble(i, data,0,data.length);
        accumulator.add(i, data,0,data.length);
    }

    @Test
    public void testCanAccumulateColumns() throws Exception {
//        System.out.println(Arrays.toString(dataTypes));
        Random random = new Random(0l);
        EntryAccumulator accumulator = new ByteEntryAccumulator(null,true);

        Object[] correctData = new Object[dataTypes.length];
        MultiFieldEncoder encoder = MultiFieldEncoder.create(1);
        for(int i=0;i<dataTypes.length;i++){
            encoder.reset();
            TestType type = dataTypes[i];
            Object correct = type.generateRandom(random);
            correctData[i] = correct;
            type.load(encoder, correct, false);
            accumulate(accumulator, encoder, i, type);
        }

        byte[] bytes = accumulator.finish();

        //get index offset
        int offset;
        for(offset=0;offset<bytes.length && bytes[offset]!=0;offset++);

        //make sure index is correct
        BitIndex index = BitIndexing.wrap(bytes,0,offset);
        for(int i=0;i<dataTypes.length;i++){
            Assert.assertTrue("Index missing entry "+ i,index.isSet(i));
            TestType type = dataTypes[i];
            if(type.isScalarType()){
                Assert.assertTrue("Incorrect type, wanted scalar",index.isScalarType(i));
            } else if(type == TestType.FLOAT){
                Assert.assertTrue("Incorrect type, wanted float",index.isFloatType(i));
            } else if(type == TestType.DOUBLE){
                Assert.assertTrue("Incorrect type, wanted double",index.isDoubleType(i));
            }else{
                //make sure it's not any of the three types
                Assert.assertFalse("Incorrect type, wanted untyped, got scalar", index.isScalarType(i));
                Assert.assertFalse("Incorrect type, wanted untyped, got float", index.isFloatType(i));
                Assert.assertFalse("Incorrect type, wanted untyped, got double", index.isDoubleType(i));
            }
        }

        MultiFieldDecoder decoder =  MultiFieldDecoder.wrap(bytes,offset+1,bytes.length-offset-1);
        //make sure they all decode correctly
        for(int i=0;i<dataTypes.length;i++){
            Object correct = correctData[i];
            TestType type = dataTypes[i];
            type.check(decoder,correct,false);
        }
    }



}
