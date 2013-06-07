package com.splicemachine.encoding;

import com.google.common.collect.Lists;
import com.gotometrics.orderly.LongRowKey;
import org.apache.hadoop.hbase.util.Bytes;
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
public class IntegerEncoderTest {

    private static final int numTests=10;
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
        return paramSet;
    }

    private int[] data;

    public IntegerEncoderTest(int[] data) {
        this.data = data;
    }

    @Test
    public void testIntToBytesAndBackAgain() throws Exception {
        //makes sure we can serialize and deserialize correctly
        for(int datum:data){
            byte[] serialized = ScalarEncoding.toBytes(datum, false);
            long deserialized = ScalarEncoding.getInt(serialized, false);
            Assert.assertEquals("Incorrect deserialization of value "+ datum, datum,deserialized);
        }
    }

    @Test
    public void testMatchesOrderly() throws Exception{
        //test that our output matches that of Orderly
        LongRowKey rowKey = new LongRowKey();
        for(int datum:data){
            byte[] ours = ScalarEncoding.toBytes(datum, false);
            byte[] correct = rowKey.serialize((long)datum);
            Assert.assertArrayEquals("Does not match orderly! Data point: "+ datum,correct,ours);
        }
    }

    @Test
    public void testBytesOrderSameAsIntOrder() throws Exception {
        //randomize the elements in the original
        byte[][] dataElements = new byte[data.length][];
        for(int pos=0;pos<data.length;pos++){
            dataElements[pos] = ScalarEncoding.toBytes(data[pos], false);
        }

        //sort dataElements
        Arrays.sort(dataElements, Bytes.BYTES_COMPARATOR);

        //deserialize
        int[] sortedData = new int[data.length];
        for(int pos=0;pos<dataElements.length;pos++){
            sortedData[pos] = ScalarEncoding.getInt(dataElements[pos], false);
        }

        //sort the original data set
        Arrays.sort(data);
        //compare the two arrays
        Assert.assertTrue("Incorrect sort!",Arrays.equals(data,sortedData));
    }
}
