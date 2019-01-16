/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.access.impl.data;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jleach on 1/3/17.
 */
public class UnsafeRecordUtilsTest {

    @Test
    public void testCardinality() {
        int n = 9999;
        int width = UnsafeRecordUtils.calculateBitSetWidthInBytes(n);
        byte[] bitSet = new byte[width+16];
        for (int i =0; i< n; i++) {
            if (i%2==0)
                UnsafeRecordUtils.set(bitSet,16,i);
        }
        Assert.assertEquals("Cardinality is not accurate",5000,UnsafeRecordUtils.cardinality(bitSet,16,width/8));

    }

    @Test
    public void testDisplayBitSet() {
        int n = 10;
        int width = UnsafeRecordUtils.calculateBitSetWidthInBytes(n);
        byte[] bitSet = new byte[width+16];
        for (int i =0; i< n; i++) {
            if (i%2==0)
                UnsafeRecordUtils.set(bitSet,16,i);
        }
        Assert.assertEquals("Display is not accurate","{0, 2, 4, 6, 8}",UnsafeRecordUtils.displayBitSet(bitSet,16,width/8));


    }

    @Test
    public void orTest() {
        int n = 10;
        int width = UnsafeRecordUtils.calculateBitSetWidthInBytes(100);
        byte[] src = new byte[width];
        byte[] or = new byte[width];
        for (int i =0; i< n; i++) {
            if (i%2==0)
                UnsafeRecordUtils.set(src,16,i);
            else
                UnsafeRecordUtils.set(or,16,i);
        }
        UnsafeRecordUtils.or(src,16,width/8,or,16,width/8);
        Assert.assertEquals("Display is not accurate","{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}",UnsafeRecordUtils.displayBitSet(src,16,width/8));
    }
}
