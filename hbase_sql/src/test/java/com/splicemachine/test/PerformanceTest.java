package com.splicemachine.test;

import org.apache.spark.sql.types.Decimal;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Created by jleach on 9/21/17.
 */
public class PerformanceTest {

    @Test
    public void testDecimal() {
        for (int i = 0; i< 10000000; i ++) {
            BigInteger bigInteger = new BigInteger(""+i);
            BigDecimal dec = new BigDecimal(bigInteger);
            BigInteger bigInteger2 = new BigInteger(""+i+1);
            BigDecimal dec2 = new BigDecimal(bigInteger2);
            BigDecimal dec3 = dec.multiply(dec2);
        }


    }

    @Test
    public void testSparkDecimal() {
        for (int i = 0; i< 10000000; i ++) {
            Decimal dec = Decimal.apply((long)i);
            Decimal dec2 = Decimal.apply((long)i+1);
            Decimal dec3 = dec.$times(dec2);
        }
    }

    @Test
    public void testZSparkDecimal() {
        for (int i = 0; i< 10000000; i ++) {
            Decimal dec = Decimal.apply((long)i);
            Decimal dec2 = Decimal.apply((long)i+1);
            Decimal dec3 = dec.$times(dec2);
        }
    }

}
