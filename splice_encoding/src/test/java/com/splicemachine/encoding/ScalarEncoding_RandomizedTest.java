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

package com.splicemachine.encoding;

import org.spark_project.guava.collect.Lists;
import com.splicemachine.primitives.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 * @author Scott Fines
 * Created on: 6/6/13
 */
@RunWith(Parameterized.class)
public class ScalarEncoding_RandomizedTest {

    private static final int numTests=100;
    private static final int testSize=100;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters(){
        Random random = new Random(System.currentTimeMillis());
        Collection<Object[]> paramSet = Lists.newArrayList();
        for(int i=0;i<numTests;i++){
            int[] testData = new int[testSize];
            for(int k=0;k<testSize;k++){
                testData[k] = random.nextInt();
            }
            paramSet.add(new Object[]{testData});
        }

        //add the fixed parameters--these are edge case values that should always be checked
        paramSet.add(new Object[]{
            new int[]{-1,0},
        });
        paramSet.add(new Object[]{
                new int[]{4607,4608},
        });

        return paramSet;
    }

    private int[] data;

    public ScalarEncoding_RandomizedTest(int[] data) {
        this.data = data;
    }

    @Test
    public void testIntToBytesAndBackAgain() throws Exception {
        //makes sure we can serialize and deserialize correctly
        for(int datum:data){
            byte[] serialized = ScalarEncoding.writeLong(datum,false);
            long deserialized = ScalarEncoding.readInt(serialized,false);
            Assert.assertEquals("Incorrect deserialization of value "+ datum, datum,deserialized);
        }
    }

    @Test
    public void testBytesOrderSameAsIntOrder() throws Exception {
        //randomize the elements in the original
        byte[][] dataElements = new byte[data.length][];
        for(int pos=0;pos<data.length;pos++){
            dataElements[pos] = ScalarEncoding.writeLong(data[pos],false);
        }

        //sort dataElements
        Arrays.sort(dataElements, Bytes.BASE_COMPARATOR);

        //deserialize
        int[] sortedData = new int[data.length];
        for(int pos=0;pos<dataElements.length;pos++){
            sortedData[pos] = ScalarEncoding.readInt(dataElements[pos],false);
        }

        //sort the original data set
        Arrays.sort(data);
        //compare the two arrays
        Assert.assertTrue("Incorrect sort!",Arrays.equals(data,sortedData));
    }
}

