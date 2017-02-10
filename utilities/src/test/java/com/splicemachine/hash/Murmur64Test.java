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
