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
 * Created on: 6/7/13
 */
@RunWith(Parameterized.class)
public class FloatEncodingTest {
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

    public FloatEncodingTest(float[] data) {
       this.data=data;
    }

    @Test
    public void testCanSerializeAndDeserializeCorrectly() throws Exception {
        for(float datum:data){
            byte[] bits = DecimalEncoding.toBytes(datum,false);
            float deser = DecimalEncoding.toFloat(bits,false);
            Assert.assertEquals("Incorrect encoding for value "+ datum,datum,deser,Math.pow(10,-6));
        }
    }

    @Test
    public void testCanSerializeAndDeserializeByteBuffersCorrectly() throws Exception {
        for(float datum:data){
            byte[] bits = DecimalEncoding.toBytes(datum,false);
            ByteBuffer wrap = ByteBuffer.wrap(bits);
            float deser = DecimalEncoding.toFloat(wrap,false);
            Assert.assertEquals("Incorrect encoding for value "+ datum,datum,deser,Math.pow(10,-6));
        }
    }

    @Test
    public void testSortsBytesCorrectly() throws Exception {
        byte[][] dataElements = new byte[data.length][];
        for(int pos=0;pos<data.length;pos++){
            dataElements[pos] = DecimalEncoding.toBytes(data[pos],false);
        }

        Arrays.sort(dataElements, Bytes.BYTES_COMPARATOR);

        float[] newData = new float[dataElements.length];
        for(int i=0;i<dataElements.length;i++){
            newData[i] = DecimalEncoding.toFloat(dataElements[i],false);
        }

        Arrays.sort(data);

        Assert.assertArrayEquals("incorrect sort ordering!",data,newData,(float)Math.pow(10,-6));
    }

    @Test
    public void testSortsBytesCorrectlyReversed() throws Exception {
        byte[][] dataElements = new byte[data.length][];
        for(int pos=0;pos<data.length;pos++){
            dataElements[pos] = DecimalEncoding.toBytes(data[pos],true);
        }

        Arrays.sort(dataElements, Bytes.BYTES_COMPARATOR);

        float[] newData = new float[dataElements.length];
        for(int i=0;i<dataElements.length;i++){
            newData[i] = DecimalEncoding.toFloat(dataElements[i],true);
        }

        Arrays.sort(data);
        float[] reversed = new float[data.length];
        for(int i=data.length-1,j=0;i>=0;i--,j++){
            reversed[j] = data[i];
        }

        Assert.assertArrayEquals("incorrect sort ordering!",reversed,newData,(float)Math.pow(10,-6));
    }
}
