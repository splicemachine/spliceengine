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

package com.splicemachine.primitives;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Random;

@RunWith(Parameterized.class)
public class BigEndianBitsTest {
    private static final int numIterations = 10;
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Random random = new Random(0l);
        Collection<Object[]> data = Lists.newArrayList();
        for(int i=0;i<numIterations;i++){
            data.add(new Object[]{random.nextInt(),random.nextLong()});
        }
        return data;
    }

    private int iValue;
    private long lValue;

    public BigEndianBitsTest(int iValue, long lValue) {
        this.iValue = iValue;
        this.lValue = lValue;
    }


    @Test
    public void testCanEncodeAndDecodeShort() throws Exception {
        byte[] encoded = BigEndianBits.toBytes((short)iValue);
        short decoded = BigEndianBits.toShort(encoded);
        Assert.assertEquals("Encoded and decoded not correct!",(short)iValue,decoded);
    }

    @Test
    public void testCanEncodeAndDecodeInt() throws Exception {
        byte[] encoded = BigEndianBits.toBytes(iValue);
        int decoded = BigEndianBits.toInt(encoded);
        Assert.assertEquals("Encoded and decoded not correct!",iValue,decoded);
    }

    @Test
    public void testCanEncodeAndDecodeLong() throws Exception {
        byte[] encoded = BigEndianBits.toBytes(lValue);
        long decoded = BigEndianBits.toLong(encoded);
        Assert.assertEquals("Encoded and decoded not correct!",lValue,decoded);
    }
}
