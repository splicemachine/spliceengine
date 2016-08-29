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

package com.splicemachine.primitives;

import org.spark_project.guava.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Random;

@RunWith(Parameterized.class)
public class BigEndianBitsTest {
    private static final int numIterations = 10;
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Random random = new Random(0l);
        Collection<Object[]> data = Lists.newArrayList();
        for(int i=0;i<numIterations;i++){
            data.add(new Object[]{random.nextInt(),random.nextLong()});
        }
        return data;
    }

    private int iValue;
    private long lValue;

    public BigEndianBitsTest(int iValue, long lValue) {
        this.iValue = iValue;
        this.lValue = lValue;
    }


    @Test
    public void testCanEncodeAndDecodeShort() throws Exception {
        byte[] encoded = BigEndianBits.toBytes((short)iValue);
        short decoded = BigEndianBits.toShort(encoded);
        Assert.assertEquals("Encoded and decoded not correct!",(short)iValue,decoded);
    }

    @Test
    public void testCanEncodeAndDecodeInt() throws Exception {
        byte[] encoded = BigEndianBits.toBytes(iValue);
        int decoded = BigEndianBits.toInt(encoded);
        Assert.assertEquals("Encoded and decoded not correct!",iValue,decoded);
    }

    @Test
    public void testCanEncodeAndDecodeLong() throws Exception {
        byte[] encoded = BigEndianBits.toBytes(lValue);
        long decoded = BigEndianBits.toLong(encoded);
        Assert.assertEquals("Encoded and decoded not correct!",lValue,decoded);
    }
}