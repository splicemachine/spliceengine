package com.splicemachine.encoding;

import com.google.common.collect.Lists;
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
 *         Created on: 6/7/13
 */
@RunWith(Parameterized.class)
public class StringEncodingTest {

    public static final int numTestsToRun=100;
    public static final int numStringsToCheckPerTest = 10;
    public static final int maxStringLength = 100;

    private static final String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789?;:,.~'\"[]{}\\|/";

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() throws Exception{
        Random random = new Random();
        Collection<Object[]> data = Lists.newArrayList();
        for(int i=0;i<numTestsToRun;i++){
            String[] dataToCheck = new String[numStringsToCheckPerTest];
            for(int j=0;j<dataToCheck.length;j++){
                char[] stringData = new char[random.nextInt(maxStringLength)+1];
                for(int pos=0;pos<stringData.length;pos++){
                    stringData[pos ] = chars.charAt(random.nextInt(chars.length()));
                }
                dataToCheck[j] = new String(stringData);
            }
            data.add(new Object[]{dataToCheck});
        }
        return data;
    }

    private final String[] dataToCheck;

    public StringEncodingTest(String[] dataToCheck) {
        this.dataToCheck = dataToCheck;
    }

    @Test
    public void testStringsCanSerializeAndDeserializeCorrectly() throws Exception {
        for(String datum:dataToCheck){
            byte[] encoding = StringEncoding.toBytes(datum, false);
            String decoding = StringEncoding.getString(encoding, false);
            Assert.assertEquals("Incorrect serialization/deserialization",datum,decoding);
        }
    }

    @Test
    public void testStringsSortCorrectly() throws Exception {
        byte[][] dataElements = new byte[dataToCheck.length][];
        for(int pos=0;pos<dataToCheck.length;pos++){
            dataElements[pos] = StringEncoding.toBytes(dataToCheck[pos], false);
        }

        Arrays.sort(dataElements, Bytes.BYTES_COMPARATOR);

        //deserialize and check equality
        String[] deserData = new String[dataElements.length];
        for(int pos=0;pos<dataElements.length;pos++){
            deserData[pos] = StringEncoding.getString(dataElements[pos], false);
        }

        //sort the original data
        Arrays.sort(dataToCheck);

        Assert.assertTrue("Sort incorrect!",Arrays.equals(dataToCheck,deserData));

    }
}
