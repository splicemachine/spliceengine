package com.splicemachine.encoding;

import com.google.common.collect.Lists;
import com.splicemachine.testutil.RandomDerbyDecimalBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

/*
 * Test BigDecimalEncoding with random values.
 */
@RunWith(Parameterized.class)
public class BigDecimalEncoding_RandomizedTest {

    private static final int NUM_TESTS = 50;
    private static final int NUM_VALUES_PER_TEST = 1000;
    private static final RandomDerbyDecimalBuilder DERBY_DECIMAL_BUILDER = new RandomDerbyDecimalBuilder().withNegatives(true);

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        Collection<Object[]> data = Lists.newArrayListWithCapacity(NUM_TESTS);
        for (int i = 0; i < NUM_TESTS; i++) {
            data.add(new Object[]{DERBY_DECIMAL_BUILDER.buildArray(NUM_VALUES_PER_TEST)});
        }
        return data;
    }

    private final BigDecimal[] data;

    public BigDecimalEncoding_RandomizedTest(BigDecimal[] data) {
        this.data = data;
    }

    @Test
    public void testCanSerializeAndDeserializeCorrectly() throws Exception {

        final boolean DESCENDING = true;
        final boolean ASCENDING = false;

        for(BigDecimal decimal:data){

            // encode/decode, descending
            byte[] b1 = BigDecimalEncoding.toBytes(decimal, DESCENDING);
            BigDecimal d1 = BigDecimalEncoding.toBigDecimal(b1, DESCENDING);
            assertTrue(decimal.compareTo(d1) == 0);

            // encode/decode, descending, NEGATIVE
            byte[] b2 = BigDecimalEncoding.toBytes(decimal.negate(), DESCENDING);
            BigDecimal d2  = BigDecimalEncoding.toBigDecimal(b2, DESCENDING);
            assertTrue(decimal.negate().compareTo(d2) == 0);

            // encode/decode, ascending
            byte[] b3 = BigDecimalEncoding.toBytes(decimal, ASCENDING);
            BigDecimal d3 = BigDecimalEncoding.toBigDecimal(b3, ASCENDING);
            assertTrue(decimal.compareTo(d3) == 0);

            // encode/decode, ascending, NEGATIVE
            byte[] b4 = BigDecimalEncoding.toBytes(decimal.negate(), ASCENDING);
            BigDecimal d4 = BigDecimalEncoding.toBigDecimal(b4, ASCENDING);
            assertTrue(decimal.negate().compareTo(d4) == 0);

        }
    }

    @Test
    public void testSortsBytesCorrectly() throws Exception {
        List<byte[]> serializedDecimals = Lists.newArrayList();
        for (BigDecimal aData : data) {
            serializedDecimals.add(BigDecimalEncoding.toBytes(aData, false));
        }

        Collections.sort(serializedDecimals, Bytes.BYTES_COMPARATOR);

        //deserialize
        BigDecimal last = null;
        BigDecimal current;
        for (byte[] serializedBytes : serializedDecimals) {
            current = BigDecimalEncoding.toBigDecimal(serializedBytes, false);
            assertTrue(String.format("last='%s', current='%s'", last, current), last == null || current.compareTo(last) >= 0);
            last = current;
        }
    }

    @Test
    public void testCanDecodeWhenManuallyConverted() throws Exception {
        /*
         * Some bits of code will manually convert from ascending to descending and back, this
         * makes sure that BigDecimals work correctly in that situation
         */

        for(BigDecimal testNum:data){
            byte[] bigDecBytes = Encoding.encode(testNum, false);

            BigDecimal result = Encoding.decodeBigDecimal(convertToDescending(bigDecBytes), true);

            assertTrue(result.compareTo(testNum) == 0);

            //check the negation as well
            BigDecimal t = testNum.negate();
            bigDecBytes = Encoding.encode(t, false);

            result = Encoding.decodeBigDecimal(convertToDescending(bigDecBytes), true);

            assertTrue(result.compareTo(t) == 0);
        }

    }

    private byte[] convertToDescending(byte[] bytes){
        byte[] retBytes = new byte[bytes.length];
        System.arraycopy(bytes,0,retBytes,0,bytes.length);
        for(int i=0;i<retBytes.length;i++){
            retBytes[i] ^=0xff;
        }

        return retBytes;
    }
}
