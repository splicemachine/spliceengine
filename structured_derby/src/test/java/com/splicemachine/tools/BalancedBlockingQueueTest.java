package com.splicemachine.tools;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import javax.annotation.Nullable;
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
                = new BalancedBlockingQueue<Integer>(2,2,new Function<Integer, Integer>() {
            @Override
            public Integer apply(@Nullable Integer input) {
                return input;
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
