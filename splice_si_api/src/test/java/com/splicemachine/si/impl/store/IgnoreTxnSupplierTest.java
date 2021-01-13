/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.si.impl.store;

import com.splicemachine.si.testenv.ArchitectureIndependent;
import com.splicemachine.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashSet;
import java.util.Set;

@Category(ArchitectureIndependent.class)
public class IgnoreTxnSupplierTest {

    @Test
    public void testCombineOverlappingRanges() {
        Set<Pair<Long, Long>> input = new HashSet<>();
        Pair<Long, Long> a = new Pair<>(0L, 1L);
        input.add(a);
        Pair<Long, Long> b = new Pair<>(2L, 3L);
        input.add(b);
        input.add( new Pair<>(4L, 6L) );
        input.add( new Pair<>(5L, 7L) );
        input.add( new Pair<>(8L, 10L) );
        input.add( new Pair<>(10L, 12L) );
        input.add( new Pair<>(11L, 13L) );

        Set<Pair<Long, Long>> output = IgnoreTxnSupplier.combineOverlappingRanges(input);

        Assert.assertEquals("Unexpected size of output " + output, 4, output.size());
        Assert.assertTrue(""+a+" is not in "+output, output.contains(a) );
        Assert.assertTrue(""+b+" is not in "+output, output.contains(b) );
        Pair<Long, Long> c = new Pair<>(4L, 7L);
        Assert.assertTrue(""+c+" is not in "+output, output.contains(c) );
        Pair<Long, Long> d = new Pair<>(8L, 13L);
        Assert.assertTrue(""+d+" is not in "+output, output.contains(d) );
    }
}
