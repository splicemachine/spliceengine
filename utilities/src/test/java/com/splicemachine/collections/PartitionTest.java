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

package com.splicemachine.collections;

import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.List;

import com.splicemachine.collections.Partition;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author P Trolard
 *         Date: 25/10/2013
 */
public class PartitionTest {

    public static List testColl = Arrays.asList(10, 20, 30, 40, 50, 60);

    public static List l(Object... args){ return Arrays.asList(args);}

    @Test
    public void testPartitions() throws Exception {
        Assert.assertEquals(l(Iterables.toArray(Partition.partition(testColl, 2), Object.class)),
                l(l(10, 20),
                        l(30, 40),
                        l(50, 60)));

        Assert.assertEquals(l(Iterables.toArray(Partition.partition(testColl, 2, 1), Object.class)),
                l(l(10, 20),
                  l(20, 30),
                  l(30, 40),
                  l(40, 50),
                  l(50, 60),
                  l(60)));

        Assert.assertEquals(l(Iterables.toArray(Partition.partition(testColl, 2, 1, true), Object.class)),
                l(l(10, 20),
                  l(20, 30),
                  l(30, 40),
                  l(40, 50),
                  l(50, 60),
                  l(60, null)));

        Assert.assertEquals(l(Iterables.toArray(Partition.partition(testColl, 2, 3, true), Object.class)),
                l(l(10, 20),
                  l(40, 50)));

        Assert.assertEquals(l(Iterables.toArray(Partition.partition(testColl, 3, 4, true), Object.class)),
                l(l(10, 20, 30),
                  l(50, 60, null)));

        Assert.assertEquals(l(Iterables.toArray(Partition.partition(testColl, 3, 4), Object.class)),
                l(l(10, 20, 30),
                  l(50, 60)));

        Assert.assertEquals(l(Iterables.toArray(Partition.partition(testColl, 3, 10, true), Object.class)),
                l(l(10, 20, 30)));

    }

}
