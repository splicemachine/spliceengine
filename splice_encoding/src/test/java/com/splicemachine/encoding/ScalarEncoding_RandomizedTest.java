/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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

