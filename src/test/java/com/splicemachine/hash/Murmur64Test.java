package com.splicemachine.hash;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
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
        }
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

        Assert.assertEquals("Incorrect int hash!",correct,actual);
    }
}
