package com.splicemachine.storage.index;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.BitSet;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 7/8/13
 */
public class SparseBitIndexTest {

    @Test
    public void testCorrectHandCheck() throws Exception {
        //makes sure it lines up to the hand constructed entities
        Map<byte[],Integer> correctReps = Maps.newHashMap();
        correctReps.put(new byte[]{0x04}, 1);
        correctReps.put(new byte[]{0x02,(byte)0x80}, 2);
        correctReps.put(new byte[]{0x02,(byte)0xC0}, 3);
        correctReps.put(new byte[]{0x03,(byte)0x80}, 4);
        correctReps.put(new byte[]{0x03,(byte)0xA0}, 5);
        correctReps.put(new byte[]{0x03,(byte)0xC0}, 6);
        correctReps.put(new byte[]{0x03,(byte)0xE0}, 7);
        correctReps.put(new byte[]{0x01,(byte)0x80}, 8);
        correctReps.put(new byte[]{0x01,(byte)0x84}, 9);
        correctReps.put(new byte[]{0x01,(byte)0x88}, 10);
        correctReps.put(new byte[]{0x01,(byte)0x8C}, 11);
        correctReps.put(new byte[]{0x01,(byte)0x90}, 12);
        correctReps.put(new byte[]{0x01,(byte)0x94}, 13);
        correctReps.put(new byte[]{0x01,(byte)0x98}, 14);
        correctReps.put(new byte[]{0x01,(byte)0x9C}, 15);
        correctReps.put(new byte[]{0x01,(byte)0xA0}, 16);
        correctReps.put(new byte[]{0x01,(byte)0xA2}, 17);

        for(byte[] bytees:correctReps.keySet()){
            SparseBitIndex index = SparseBitIndex.wrap(bytees,0,bytees.length);
            Assert.assertTrue("Incorrect bytes with index "+ index,index.isSet(correctReps.get(bytees)));
            Assert.assertEquals(1,index.cardinality());
        }
    }

    @Test
    public void testEncodesCorrectlyHandCheck() throws Exception {
        //makes sure it lines up to the hand constructed entities
        Map<Integer,byte[]> correctReps = Maps.newHashMap();
        correctReps.put(1,new byte[]{0x04});
        correctReps.put(2,new byte[]{0x02,(byte)0x80});
        correctReps.put(3,new byte[]{0x02,(byte)0xC0});
        correctReps.put(4,new byte[]{0x03,(byte)0x80});
        correctReps.put(5,new byte[]{0x03,(byte)0xA0});
        correctReps.put(6,new byte[]{0x03,(byte)0xC0});
        correctReps.put(7,new byte[]{0x03,(byte)0xE0});
        correctReps.put(8,new byte[]{0x01,(byte)0x80});
        correctReps.put(9,new byte[]{0x01,(byte)0x84});
        correctReps.put(10,new byte[]{0x01,(byte)0x88});
        correctReps.put(11,new byte[]{0x01,(byte)0x8C});
        correctReps.put(12,new byte[]{0x01,(byte)0x90});
        correctReps.put(13,new byte[]{0x01,(byte)0x94});
        correctReps.put(14,new byte[]{0x01,(byte)0x98});
        correctReps.put(15,new byte[]{0x01,(byte)0x9C});
        correctReps.put(16,new byte[]{0x01,(byte)0xA0});
        correctReps.put(17,new byte[]{0x01,(byte)0xA2});

        for(int val:correctReps.keySet()){
            BitSet test = new BitSet();
            test.set(val);

            byte[] actual = new SparseBitIndex(test).encode();
            Assert.assertArrayEquals("Incorrect encoding for BitSet "+ test,correctReps.get(val),actual);
        }
    }


}
