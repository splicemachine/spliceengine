package com.splicemachine.encoding;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 * @author Scott Fines
 * Created on: 6/8/13
 */
@RunWith(Parameterized.class)
public class DoubleEncodingTest {
    private static final int numTests=100;
    private static final int doublesPerTest=100;
    private static final int scale=100;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() throws Exception{
        Random random = new Random();
        Collection<Object[]> data = Lists.newArrayListWithCapacity(numTests);
        for(int i=0;i<numTests;i++){
            double[] doubles = new double[doublesPerTest];
            for(int j=0;j<doublesPerTest;j++){
                int signum = random.nextBoolean()?-1:1;
                doubles[j] = random.nextDouble()*scale*signum;
            }
            data.add(new Object[]{doubles});
        }
        return data;
    }

    private final double[] data;

    public DoubleEncodingTest(double[] data) {
        this.data = data;
    }


    @Test
    public void testCanSerializeAndDeserializeCorrectly() throws Exception {
        for(double datum:data){
            byte[] test = DecimalEncoding.toBytes(datum,false);
            double ret = DecimalEncoding.toDouble(test,false);
            Assert.assertEquals("Incorrect encoding",datum,ret,Math.pow(10,-12));
        }
    }

    @Test
    public void testCanSerializeAndDeserializeByteBuffersCorrectly() throws Exception {
        for(double datum:data){
            byte[] test = DecimalEncoding.toBytes(datum,false);
            double ret = DecimalEncoding.toDouble(ByteBuffer.wrap(test), false);
            Assert.assertEquals("Incorrect encoding",datum,ret,Math.pow(10,-12));
        }
    }

    @Test
    public void testCanSortByBytes() throws Exception {
        byte[][] dataBytes = new byte[data.length][];
        for(int pos=0;pos<data.length;pos++){
            dataBytes[pos] = DecimalEncoding.toBytes(data[pos],false);
        }

        Arrays.sort(dataBytes, Bytes.BYTES_COMPARATOR);

        double[] deserialized = new double[dataBytes.length];
        for(int pos=0;pos<deserialized.length;pos++){
            deserialized[pos] = DecimalEncoding.toDouble(dataBytes[pos],false);
        }

        Arrays.sort(data);

        Assert.assertArrayEquals("Incorrect sort ordering",data,deserialized,Math.pow(10,-12));
    }
}
