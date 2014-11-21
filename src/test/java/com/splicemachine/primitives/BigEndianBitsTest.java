package com.splicemachine.primitives;

import com.google.common.collect.Lists;
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