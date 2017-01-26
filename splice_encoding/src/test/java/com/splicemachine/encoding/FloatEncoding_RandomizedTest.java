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

/*
 * Test FloatEncoding with random values.
 */
@RunWith(Parameterized.class)
public class FloatEncoding_RandomizedTest {
    private static final int numTests=100;
    private static final int scale = 10;
    private static final int numFloatsPerTest = 10;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() throws Exception{
        Random random = new Random();
        Collection<Object[]> data = Lists.newArrayListWithCapacity(10);
        for(int i=0;i<numTests;i++){
            float[] dataToCheck = new float[numFloatsPerTest];
            for(int j=0;j<dataToCheck.length;j++){
                int sign = random.nextBoolean()? -1: 1;
                dataToCheck[j] = random.nextFloat()*scale*sign;
            }
            data.add(new Object[]{dataToCheck});
        }
        return data;
    }

    private final float[] data;

    public FloatEncoding_RandomizedTest(float[] data) {
       this.data=data;
    }

    @Test
    public void testCanSerializeAndDeserializeCorrectly() throws Exception {
        for(float datum:data){
            EncodingTestUtil.assertEncodeDecode(datum);
        }
    }

    @Test
    public void testSortsBytesCorrectly() throws Exception {
        byte[][] dataElements = new byte[data.length][];
        for(int pos=0;pos<data.length;pos++){
            dataElements[pos] = FloatEncoding.toBytes(data[pos], false);
        }

        Arrays.sort(dataElements, Bytes.BASE_COMPARATOR);

        float[] newData = new float[dataElements.length];
        for(int i=0;i<dataElements.length;i++){
            newData[i] = FloatEncoding.toFloat(dataElements[i], false);
        }

        Arrays.sort(data);

        Assert.assertArrayEquals("incorrect sort ordering!",data,newData,(float)Math.pow(10,-6));
    }

    @Test
    public void testSortsBytesCorrectlyReversed() throws Exception {
        byte[][] dataElements = new byte[data.length][];
        for(int pos=0;pos<data.length;pos++){
            dataElements[pos] = FloatEncoding.toBytes(data[pos], true);
        }

        Arrays.sort(dataElements, Bytes.BASE_COMPARATOR);

        float[] newData = new float[dataElements.length];
        for(int i=0;i<dataElements.length;i++){
            newData[i] = FloatEncoding.toFloat(dataElements[i], true);
        }

        Arrays.sort(data);
        float[] reversed = new float[data.length];
        for(int i=data.length-1,j=0;i>=0;i--,j++){
            reversed[j] = data[i];
        }

        Assert.assertArrayEquals("incorrect sort ordering!",reversed,newData,(float)Math.pow(10,-6));
    }
}
