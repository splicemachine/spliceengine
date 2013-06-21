package com.splicemachine.encoding;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 * @author Scott Fines
 * Created on: 6/7/13
 */
@RunWith(Parameterized.class)
public class RandomizedDecimalEncodingTest {
    private static final int numTests=50;
    private static final int numValuesPerTest=1000;
    private static final int maxSizePerDecimal=17;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        Random random = new Random();
        Collection<Object[]> data = Lists.newArrayListWithCapacity(numTests);
        for(int i=0;i<numTests;i++){
            BigDecimal[] values = new BigDecimal[numValuesPerTest];
            for(int j=0;j<values.length;j++){
                values[j] = new BigDecimal(new BigInteger(maxSizePerDecimal,random),random.nextInt(maxSizePerDecimal));
            }
            data.add(new Object[]{values});
        }
        return data;
    }

    private final BigDecimal[] data;

    public RandomizedDecimalEncodingTest(BigDecimal[] data) {
        this.data = data;
    }

    @Test
    public void testCanSerializeAndDeserializeCorrectly() throws Exception {
        for(BigDecimal decimal:data){
            byte[] data = DecimalEncoding.toBytes(decimal,false);
            BigDecimal ret = DecimalEncoding.toBigDecimal(data,false);

            Assert.assertTrue(decimal.compareTo(ret)==0);
        }
    }

    @Test
    public void testCanSerializeAndDeserializeByteBuffersCorrectly() throws Exception {
        for(BigDecimal decimal:data){
            byte[] data = DecimalEncoding.toBytes(decimal,false);
            BigDecimal ret = DecimalEncoding.toBigDecimal(ByteBuffer.wrap(data), false);

            Assert.assertTrue("Incorrect serialization of value " + decimal,ret.compareTo(decimal)==0);
        }
    }

    @Test
    public void testSortsBytesCorrectly() throws Exception {
        byte[][] serData = new byte[data.length][];
        for(int pos=0;pos<data.length;pos++){
            serData[pos] = DecimalEncoding.toBytes(data[pos],false);
        }

        Arrays.sort(serData, Bytes.BYTES_COMPARATOR);

        //deserialize
        BigDecimal[] deDat = new BigDecimal[serData.length];
        for(int pos=0;pos<deDat.length;pos++){
            deDat[pos] = DecimalEncoding.toBigDecimal(serData[pos],false);
        }

        for(int dePos=0;dePos<deDat.length;dePos++){
            BigDecimal toCompare = deDat[dePos];
            for(int i=0;i<dePos;i++){
                BigDecimal lessThan = deDat[i];
                Assert.assertTrue("Incorrect sort at position "+ dePos,lessThan.compareTo(toCompare)<=0);
            }
            for(int i=dePos+1;i<deDat.length;i++){
                BigDecimal greaterThan = deDat[i];
                Assert.assertTrue("Incorrect sort at position " + dePos,greaterThan.compareTo(toCompare)>=0);
            }
        }

    }
}
