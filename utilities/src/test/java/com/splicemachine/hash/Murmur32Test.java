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

package com.splicemachine.hash;

import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.hash.HashCode;
import org.spark_project.guava.hash.Hashing;
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
 *         Created on: 11/2/13
 */
@RunWith(Parameterized.class)
public class Murmur32Test {
    private static final int maxRuns=1000;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> data = Lists.newArrayListWithCapacity(100);
        Random random = new Random(0l);
        for(int i=0;i<maxRuns;i++){
            byte[] dataPoint = new byte[i+1];
            random.nextBytes(dataPoint);
            int dataPointOffset = random.nextInt(dataPoint.length);
            int dataPointLength = random.nextInt(dataPoint.length-dataPointOffset);
            data.add(new Object[]{dataPoint,random.nextInt(), dataPointOffset,dataPointLength, random.nextLong()});
        }
        return data;
    }

    private final byte[] sampleData;
    private final int sampleOffset;
    private final int sampleLength;
    private final int sampleValue;
    private final long sampleLong;
    private final Murmur32 murmur32 = new Murmur32(0);

    public Murmur32Test(byte[] sampleData,int sampleValue,int sampleOffset,int sampleLength,long sampleLong) {
        this.sampleData = sampleData;
        this.sampleValue = sampleValue;
        this.sampleLong = sampleLong;
        this.sampleOffset = sampleOffset;
        this.sampleLength = sampleLength;
    }

    @Test
    public void testMatchesGoogleVersionMurmur332() throws Exception {
        HashCode hashCode = Hashing.murmur3_32(0).hashBytes(sampleData, 0, sampleData.length);
        int actual =hashCode.asInt();

        int hash = murmur32.hash(sampleData, 0, sampleData.length);

        Assert.assertEquals(actual,hash);
    }

    @Test
    public void testMatchesGoogleVersionMurmur332SubBuffers() throws Exception {
        HashCode hashCode = Hashing.murmur3_32(0).hashBytes(sampleData, sampleOffset, sampleLength);
        int actual =hashCode.asInt();

        int hash = murmur32.hash(sampleData, sampleOffset, sampleLength);

        Assert.assertEquals(actual,hash);
    }

    @Test
    public void testByteBufferSameAsByteArray() throws Exception {
        int correct = murmur32.hash(sampleData,0,sampleData.length);
        ByteBuffer bb = ByteBuffer.wrap(sampleData);
        int actual = murmur32.hash(bb);

        Assert.assertEquals(correct,actual);
    }

    @Test
    public void testIntSameAsByteArray() throws Exception {
        byte[] bytes = Bytes.toBytes(sampleValue);
        int correct = murmur32.hash(bytes,0,bytes.length);

        int actual = murmur32.hash(sampleValue);

        Assert.assertEquals("Incorrect int hash!",correct,actual);
    }

    @Test
    public void testLongSameAsByteArray() throws Exception {
        byte[] bytes = Bytes.toBytes(sampleLong);
        int correct = murmur32.hash(bytes,0,bytes.length);

        int actual = murmur32.hash(sampleLong);

        Assert.assertEquals("Incorrect int hash!",correct,actual);
    }
}
