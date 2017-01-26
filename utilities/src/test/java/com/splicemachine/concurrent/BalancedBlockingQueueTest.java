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

package com.splicemachine.concurrent;

import org.spark_project.guava.collect.Lists;
import com.splicemachine.concurrent.BalancedBlockingQueue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Single-threaded correctness tests for priority ordering
 *
 * @author Scott Fines
 * Created on: 4/12/13
 */
public class BalancedBlockingQueueTest {

    @Test
    public void testPriorityOrdering() throws Exception{
        BalancedBlockingQueue<Integer> queue
                = new BalancedBlockingQueue<Integer>(2,2,new BalancedBlockingQueue.PriorityFunction<Integer>() {
            @Override
            public int getPriority(Integer item) {
                return item;
            }
        });

        /*
         *put in 6 items: 4P2, 2P1 items, then take them off the queue into a list,
         * and check the ordering.
         */
        queue.offer(1);
        queue.offer(1);
        queue.offer(2);
        queue.offer(2);
        queue.offer(2);
        queue.offer(2);

        List<Integer> outputs = Lists.newArrayListWithCapacity(5);
        Integer next;
        while((next = queue.poll())!=null){
            outputs.add(next);
        }

        List<Integer> correct = Arrays.asList(2,2,1,2,2,1);
        Assert.assertEquals("Incorrect data size returned!",correct.size(),outputs.size());
        for(int i=0;i<correct.size();i++){
            Integer correctVal = correct.get(i);
            Integer actualVal = outputs.get(i);
            Assert.assertEquals("Incorrect output order!",correctVal,actualVal);
        }
    }

}
