package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.collections.RingBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/28/14
 */
public class DualHashHashTableTest {
    @Test
    public void testWorksAfterResize() throws Exception {
        DualHashHashTable.EntryHasher<Integer> intHasher = new DualHashHashTable.EntryHasher<Integer>() {
            @Override
            public int hash(Integer element) {
                return (element*2)%10;
            }

            @Override
            public boolean equalsOnHash(Integer left, Integer right) {
                return left.equals(right);
            }
        };
        int size = 8;
        DualHashHashTable<Integer> hashTable = new DualHashHashTable<Integer>(size,intHasher,intHasher);

        List<Integer> correct = Lists.newArrayList();
        for(int i=0;i<size;i++){
            hashTable.add(i);
            correct.add(i);
        }
        Assert.assertEquals("Incorrect size value!", size, hashTable.size());

        List<Integer> actual = Lists.newArrayListWithExpectedSize(size);
        for(int i=0;i<size;i++){
            RingBuffer<Integer> integerRingBuffer = hashTable.get(i);
            Assert.assertNotNull("No entry found for value "+ i,integerRingBuffer);
            while(integerRingBuffer.size()>0){
                actual.add(integerRingBuffer.next());
            }
        }
        Assert.assertEquals("Incorrect returned values!",correct,actual);
    }
}
