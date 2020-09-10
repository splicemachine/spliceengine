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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author Scott Fines
 * Created on: 6/8/13
 */
@RunWith(Parameterized.class)
public class DoubleEncoding_RandomizedTest {
    private static final int numTests=100;
    private static final int doublesPerTest=100;
    private static final int scale=100;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() throws Exception{
        Random random = new Random();
        Collection<Object[]> data = Lists.newArrayListWithCapacity(numTests);
        for(int i=0;i<numTests;i++){
            double[] doubles = new double[doublesPerTest];
            for(int j=0;j<doublesPerTest;j++){
                int signum = random.nextBoolean()?-1:1;
                doubles[j] = random.nextDouble()*scale*signum;
            }
            data.add(new Object[]{doubles});
        }
        return data;
    }

    private final double[] data;

    public DoubleEncoding_RandomizedTest(double[] data) {
        this.data = data;
    }


    @Test
    public void testCanSerializeAndDeserializeCorrectly() throws Exception {
        for (double datum : data) {
            EncodingTestUtil.assertEncodeDecode(datum);
        }
    }

    @Test
    public void testCanSortByBytes() throws Exception {
        byte[][] dataBytes = new byte[data.length][];
        for(int pos=0;pos<data.length;pos++){
            dataBytes[pos] = DoubleEncoding.toBytes(data[pos], false);
        }

        Arrays.sort(dataBytes, Bytes.BASE_COMPARATOR);

        double[] deserialized = new double[dataBytes.length];
        for(int pos=0;pos<deserialized.length;pos++){
            deserialized[pos] = DoubleEncoding.toDouble(dataBytes[pos], false);
        }

        Arrays.sort(data);

        assertArrayEquals("Incorrect sort ordering", data, deserialized, 1e-12);
    }

    @Test
    public void testCanSortByBytesReversed() throws Exception {
        byte[][] dataBytes = new byte[data.length][];
        for(int pos=0;pos<data.length;pos++){
            dataBytes[pos] = DoubleEncoding.toBytes(data[pos], true);
        }

        Arrays.sort(dataBytes, Bytes.BASE_COMPARATOR);

        double[] deserialized = new double[dataBytes.length];
        for(int pos=0;pos<deserialized.length;pos++){
            deserialized[pos] = DoubleEncoding.toDouble(dataBytes[pos], true);
        }

        Arrays.sort(data);
        double[] reversed = new double[data.length];
        for(int i=data.length-1,j=0;i>=0;i--,j++){
        	reversed[j] = data[i];
        }

        assertArrayEquals("Incorrect sort ordering", reversed, deserialized, 1e-12);
    }
}
