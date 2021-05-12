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
import java.util.function.Consumer;

@Category(ArchitectureIndependent.class)
public class IgnoreTxnSupplierImplTest {

    @Test
    public void testCombineOverlappingRanges() {
        Set<Pair<Long, Long>> input = new HashSet<>();
        Pair<Long, Long> a = new Pair<>(0L, 2L);
        input.add(a);
        Pair<Long, Long> b = new Pair<>(3L, 5L);
        input.add(b);

        Pair<Long, Long> c = new Pair<>(6L, 8L);
        input.add(c);
        Pair<Long, Long> d = new Pair<>(8L, 10L);
        input.add(d);

        Pair<Long, Long> e = new Pair<>(11L, 13L);
        input.add(e);
        Pair<Long, Long> f = new Pair<>(11L, 14L);
        input.add(f);

        Pair<Long, Long> g = new Pair<>(15L, 17L);
        input.add(g);
        Pair<Long, Long> h = new Pair<>(15L, 17L);
        input.add(h);
        
        Pair<Long, Long> i = new Pair<>(18L, 21L);
        input.add(i);
        Pair<Long, Long> j = new Pair<>(19L, 21L);
        input.add(j);
        
        Pair<Long, Long> k = new Pair<>(22L, 25L);
        input.add(k);
        Pair<Long, Long> l = new Pair<>(23L, 26L);
        input.add(l);

        Pair<Long, Long> m = new Pair<>(27L, 31L);
        input.add(m);
        Pair<Long, Long> n = new Pair<>(28L, 30L);
        input.add(n);

        Pair<Long, Long> q = new Pair<>(32L, 35L);
        input.add(q);
        Pair<Long, Long> r = new Pair<>(32L, 34L);
        input.add(r);
        Pair<Long, Long> s = new Pair<>(33L, 36L);
        input.add(s);

        Set<Pair<Long, Long>> output = IgnoreTxnSupplierImpl.combineOverlappingRanges(input);

        Consumer<Pair<Long,Long>> isThere = p ->
            Assert.assertTrue(""+p+" is not in "+output, output.contains(p) );

        Consumer<Pair<Long,Long>> notThere = p ->
            Assert.assertFalse(""+p+" is in "+output, output.contains(p) );

        isThere.accept(a);
        isThere.accept(b);
        isThere.accept(c);
        isThere.accept(d);
        notThere.accept(e);
        isThere.accept(f);
        isThere.accept(g);
        isThere.accept(i);
        notThere.accept(j);
        notThere.accept(k);
        notThere.accept(l);
        isThere.accept(new Pair<>(22L, 26L));
        isThere.accept(m);
        notThere.accept(n);
        notThere.accept(q);
        notThere.accept(r);
        notThere.accept(s);
        isThere.accept(new Pair<>(32L, 36L));
        
        Assert.assertEquals("Unexpected size of output " + output, 10, output.size());
    }
}
