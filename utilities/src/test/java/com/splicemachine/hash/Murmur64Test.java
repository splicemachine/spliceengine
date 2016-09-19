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

package com.splicemachine.hash;

import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.hash.HashFunction;
import com.splicemachine.primitives.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 7/20/15
 */
@RunWith(Parameterized.class)
public class Murmur64Test{
    private static final int maxRuns=1000;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> data = Lists.newArrayListWithCapacity(100);
        Random random = new Random(0l);
        for(int i=0;i<maxRuns;i++){
            byte[] dataPoint = new byte[i+1];
            random.nextBytes(dataPoint);
            data.add(new Object[]{dataPoint,random.nextInt(), random.nextLong()});
            data.add(new Object[]{dataPoint,random.nextInt(), random.nextInt()<<random.nextInt(30)});
        }
        data.add(new Object[]{new byte[2],Integer.MAX_VALUE,Long.MAX_VALUE});
        return data;
    }

    private final byte[] sampleData;
    private final int sampleValue;
    private final long sampleLong;
    private final Murmur64 hasher= new Murmur64(0);

    public Murmur64Test(byte[] sampleData,int sampleValue,long sampleLong) {
        this.sampleData = sampleData;
        this.sampleValue = sampleValue;
        this.sampleLong = sampleLong;
    }

    @Test
    public void testByteBufferSameAsByteArray() throws Exception {
        long correct = hasher.hash(sampleData,0,sampleData.length);
        ByteBuffer bb = ByteBuffer.wrap(sampleData);
        long actual = hasher.hash(bb);

        Assert.assertEquals(correct,actual);
    }

    @Test
    public void testIntSameAsByteArray() throws Exception {
        byte[] bytes = Bytes.toBytes(sampleValue);
        long correct = hasher.hash(bytes,0,bytes.length);

        long actual = hasher.hash(sampleValue);

        Assert.assertEquals("Incorrect int hash!",correct,actual);
    }

    @Test
    public void testLongSameAsByteArray() throws Exception {
        byte[] bytes = Bytes.toBytes(sampleLong);
        long correct = hasher.hash(bytes,0,bytes.length);

        long actual = hasher.hash(sampleLong);

        Assert.assertEquals("Incorrect long hash!",correct,actual);
    }
}
