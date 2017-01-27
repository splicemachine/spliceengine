/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.encoding;

import org.spark_project.guava.collect.Lists;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.testutil.RandomDerbyDecimalBuilder;
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
        for(BigDecimal decimal:data){
            EncodingTestUtil.assertEncodeDecode(decimal);
            EncodingTestUtil.assertEncodeDecode(decimal.negate());
        }
    }

    @Test
    public void testSortsBytesCorrectly() throws Exception {
        List<byte[]> serializedDecimals = Lists.newArrayList();
        for (BigDecimal aData : data) {
            serializedDecimals.add(BigDecimalEncoding.toBytes(aData, false));
        }

        Collections.sort(serializedDecimals, Bytes.BASE_COMPARATOR);

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
