/*  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package orderly;

import orderly.RowKey;
import orderly.VariableLengthBytesWritableRowKey;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class TestVariableLengthBytesWritableRowKey extends RandomRowKeyTestCase {

    @Override
    public RowKey createRowKey() {
        return new VariableLengthBytesWritableRowKey();
    }

    @Override
    public Object createObject() {
        final int length = r.nextInt(1000);
        final byte[] randomBytes = new byte[length];
        r.nextBytes(randomBytes);
        return new BytesWritable(randomBytes);
    }

    @Override
    public int compareTo(Object o1, Object o2) {
        if (o1 == null || o2 == null)
            return (o1 != null ? 1 : 0) - (o2 != null ? 1 : 0);

        BytesWritable b1 = ((BytesWritable) o1);
        BytesWritable b2 = ((BytesWritable) o2);

        final int compareTo = b1.compareTo(b2);

        return compareTo < 0 ? -1 : compareTo > 0 ? 1 : 0;
    }

    // some specific tests for the customized BCD format

    private byte[] encode(String decimalDigits, int expectedLength) {
        final ImmutableBytesWritable result = new ImmutableBytesWritable(new byte[expectedLength]);

        new VariableLengthBytesWritableRowKey().encodedCustomizedReversedPackedBcd(decimalDigits, result);

        return result.get();
    }

    private String decode(byte[] input) {
        return new VariableLengthBytesWritableRowKey().decodeCustomizedReversedPackedBcd(
                new ImmutableBytesWritable(input), 0, input.length);
    }

    @Test
    public void testEncode() {
        // 0 encodes to 0x3 + terminator nibble 0x01
        assertArrayEquals(new byte[]{0x31}, encode("0", 1));
        // etc...
        assertArrayEquals(new byte[]{0x41}, encode("1", 1));
        assertArrayEquals(new byte[]{0x51}, encode("2", 1));
        assertArrayEquals(new byte[]{0x61}, encode("3", 1));
        assertArrayEquals(new byte[]{0x71}, encode("4", 1));

        assertArrayEquals(new byte[]{0x33}, encode("00", 1));
        assertArrayEquals(new byte[]{0x34}, encode("01", 1));
        assertArrayEquals(new byte[]{0x35}, encode("02", 1));
        assertArrayEquals(new byte[]{0x36}, encode("03", 1));
        assertArrayEquals(new byte[]{0x37}, encode("04", 1));
        assertArrayEquals(new byte[]{0x39}, encode("05", 1));
        assertArrayEquals(new byte[]{0x3A}, encode("06", 1));
        assertArrayEquals(new byte[]{0x3C}, encode("07", 1));
        assertArrayEquals(new byte[]{0x3E}, encode("08", 1));
        assertArrayEquals(new byte[]{0x3F}, encode("09", 1));

        assertArrayEquals(new byte[]{0x45}, encode("12", 1));
        assertArrayEquals(new byte[]{0x45, 0x61}, encode("123", 2));
        assertArrayEquals(new byte[]{0x45, 0x67}, encode("1234", 2));

        // a special case (encoded value is 0x9E which is actually a negative byte when interpreted as signed value)
        byte expected = (byte) 0x9E; // decimal -98
        assertArrayEquals(new byte[]{expected}, encode("58", 1));
    }

    @Test
    public void testDecode() {
        assertEquals("0", decode(new byte[]{0x31}));
        assertEquals("1", decode(new byte[]{0x41}));
        assertEquals("2", decode(new byte[]{0x51}));
        assertEquals("3", decode(new byte[]{0x61}));
        assertEquals("4", decode(new byte[]{0x71}));

        assertEquals("12", decode(new byte[]{0x45}));
        assertEquals("123", decode(new byte[]{0x45, 0x61}));
        assertEquals("1234", decode(new byte[]{0x45, 0x67}));

        assertEquals("123", decode(new byte[]{0x45, 0x61, 0x00})); // stuff after termination nibble (0x01) is ignored
    }

}
