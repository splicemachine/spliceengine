package com.splicemachine.encoding;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Random;

/**
 * @author Scott Fines
 *         Created on: 6/7/13
 */
@RunWith(Parameterized.class)
public class DecimalEncodingTest {
    private static final int numTests=1;
    private static final int numValuesPerTest=100;
    private static final int maxSizePerDecimal=1000;

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

    public DecimalEncodingTest(BigDecimal[] data) {
        this.data = data;
    }

    @Test
    public void testCanSerializeAndDeserializeCorrectly() throws Exception {
        for(BigDecimal decimal:data){
            byte[] data = DecimalEncoding.toBytes(decimal,false);
            BigDecimal ret = DecimalEncoding.toBigDecimal(data,false);

            Assert.assertEquals("Incorrect serialization of value "+ decimal,decimal.stripTrailingZeros(),ret);
        }
    }
}
