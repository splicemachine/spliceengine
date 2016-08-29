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
