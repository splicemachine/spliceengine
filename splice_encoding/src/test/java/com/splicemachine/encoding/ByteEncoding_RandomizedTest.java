/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import splice.com.google.common.collect.Lists;
import com.splicemachine.primitives.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Random;

/*
 * Test ByteEncoding with random values.
 */
@RunWith(Parameterized.class)
public class ByteEncoding_RandomizedTest {
    private static final int numTests=10;
    private static final int arraysPerTest =10;
    private static final int bytesPerArray = 11200;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Random random = new Random();
        Collection<Object[]> params = Lists.newArrayListWithCapacity(numTests);
        for(int test=0;test<numTests;test++){
            byte[][] data = new byte[arraysPerTest][];
            for(int array=0;array<data.length;array++){
                byte[] elem = new byte[bytesPerArray];
                random.nextBytes(elem);
                data[array] = elem;
            }
            params.add(new Object[]{data});
        }
        return params;
    }

    private final byte[][] data;

    public ByteEncoding_RandomizedTest(byte[][] data) {
        this.data = data;
    }

    @Test
    public void testCanSerializeAndDeserializeCorrectly() throws Exception {
        for(byte[] datum:data){
            byte[] serialized = ByteEncoding.encode(datum,false);
            byte[] decoded = ByteEncoding.decode(serialized,false);

            Assert.assertArrayEquals("incorrect encoding for element "+ Arrays.toString(datum),datum,decoded);
        }
    }

    @Test
    public void testCanEncodeAndDecodeUnsortedCorrectly ()throws Exception {
        for(byte[] datum:data){
            byte[] encoded = ByteEncoding.encodeUnsorted(datum);
            byte[] decoded = ByteEncoding.decodeUnsorted(encoded,0,encoded.length);

            Assert.assertArrayEquals("Incorrect encoding/decoding",datum,decoded);
        }

        for(byte[] datum:data){
            byte[] encoded = ByteEncoding.encodeUnsorted(datum,1,datum.length-1);
            byte[] decoded = ByteEncoding.decodeUnsorted(encoded,0,encoded.length);

            byte[] dCopy = new byte[datum.length-1];
            System.arraycopy(datum,1,dCopy,0,dCopy.length);

            Assert.assertArrayEquals("Incorrect encoding/decoding",dCopy,decoded);
        }
    }

    @Test
    public void testNoZerosUnsorted() throws Exception {
        /*
         * Makes sure that there are no zeros in the encoded byte[]
         */
        for(byte[] datum:data){
            byte[] encoded = ByteEncoding.encodeUnsorted(datum);
            for(byte byt:encoded){
                Assert.assertNotEquals("Zeros found in "+ datum,0x00,byt);
            }
        }

    }

    @Test
    public void testCanSerializeAndDeserializeByteBuffersCorrectly() throws Exception {
        for(byte[] datum:data){
            byte[] serialized = ByteEncoding.encode(datum,false);
            byte[] decoded = ByteEncoding.decode(ByteBuffer.wrap(serialized), false);

            Assert.assertArrayEquals("incorrect encoding for element "+ Arrays.toString(datum),datum,decoded);
        }
    }

    @Test
    public void testSortOrderCorrect() throws Exception {
        byte[][] encoded = new byte[data.length][];
        for(int pos=0;pos<encoded.length;pos++){
            encoded[pos] = ByteEncoding.encode(data[pos],false);
        }

        Arrays.sort(encoded, Bytes.BASE_COMPARATOR);

        byte[][] decoded = new byte[encoded.length][];
        for(int pos=0;pos<encoded.length;pos++){
            decoded[pos] = ByteEncoding.decode(encoded[pos],false);
        }

        Arrays.sort(data,Bytes.BASE_COMPARATOR);
        Assert.assertArrayEquals("Incorrect sort order!",data,decoded);
    }

    @Test
    public void testReverseSortOrderCorrect() throws Exception {
        byte[][] encoded = new byte[data.length][];
        for(int pos=0;pos<encoded.length;pos++){
            encoded[pos] = ByteEncoding.encode(data[pos],true);
        }

        Arrays.sort(encoded, Bytes.BASE_COMPARATOR);

        byte[][] decoded = new byte[encoded.length][];
        for(int pos=0;pos<encoded.length;pos++){
            decoded[pos] = ByteEncoding.decode(encoded[pos],true);
        }

        //sort original array in reverse
        Arrays.sort(data,new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return -1*Bytes.BASE_COMPARATOR.compare(o1,o2);
            }
        });
        Assert.assertArrayEquals("Incorrect sort order!",data,decoded);
    }
}
