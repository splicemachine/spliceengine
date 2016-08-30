/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.collections;

import org.spark_project.guava.collect.Iterables;
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
