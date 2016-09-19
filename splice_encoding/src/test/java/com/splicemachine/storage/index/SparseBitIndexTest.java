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

package com.splicemachine.storage.index;


import com.carrotsearch.hppc.BitSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 7/8/13
 */
@RunWith(Parameterized.class)
public class SparseBitIndexTest {

    @Parameterized.Parameters
    public static Collection<Object[]> parameters(){
        List<Object[]> ds = new ArrayList<>();
        /*
         * Regression data for SPLICE-816
         */
        BitSet nnCols = new BitSet();
        nnCols.set(0,6);
        nnCols.set(7);
        nnCols.set(10);
        nnCols.set(15,22);
        nnCols.set(34,37);
        nnCols.set(43,46);
        nnCols.set(48,50);
        nnCols.set(56);
        nnCols.set(58);
        nnCols.set(62,64);
        nnCols.set(67,70);
        nnCols.set(89,91);
        nnCols.set(92);
        nnCols.set(94,96);

        BitSet scalars = new BitSet();
        scalars.set(0);
        scalars.set(3);
        scalars.set(34,37);
        scalars.set(44);
        scalars.set(63);
        scalars.set(68);
        scalars.set(90);
        scalars.intersect(nnCols);

        ds.add(new Object[]{nnCols,scalars,new BitSet(),new BitSet()});

        return ds;
    }
    private BitSet nnCols;
    private BitSet scalars;
    private BitSet doubles;
    private BitSet floats;

    public SparseBitIndexTest(BitSet nnCols,BitSet scalars,BitSet doubles,BitSet floats){
        this.nnCols=nnCols;
        this.scalars=scalars;
        this.doubles=doubles;
        this.floats=floats;
    }

    @Test
    public void testSparseBitIndexHasNoZeros() throws Exception{

        BitIndex bi = SparseBitIndex.create(nnCols,scalars,doubles,floats);
        byte[] d = bi.encode();
        for(int i=0;i<d.length;i++){
            Assert.assertNotEquals("Contained a 0 at pos <"+i+">!",0x00,d[i]);
        }
        BitIndex nbi = SparseBitIndex.wrap(d,0,d.length);
        Assert.assertEquals("Incorrect deserialization!",bi,nbi);
    }
    //    @Test
//    public void testCorrectHandCheck() throws Exception {
//        //makes sure it lines up to the hand constructed entities
//        Map<byte[],Integer> correctReps = Maps.newHashMap();
//        correctReps.put(new byte[]{0x04}, 1);
//        correctReps.put(new byte[]{0x02,(byte)0x80}, 2);
//        correctReps.put(new byte[]{0x02,(byte)0xC0}, 3);
//        correctReps.put(new byte[]{0x03,(byte)0x80}, 4);
//        correctReps.put(new byte[]{0x03,(byte)0xA0}, 5);
//        correctReps.put(new byte[]{0x03,(byte)0xC0}, 6);
//        correctReps.put(new byte[]{0x03,(byte)0xE0}, 7);
//        correctReps.put(new byte[]{0x01,(byte)0x80}, 8);
//        correctReps.put(new byte[]{0x01,(byte)0x84}, 9);
//        correctReps.put(new byte[]{0x01,(byte)0x88}, 10);
//        correctReps.put(new byte[]{0x01,(byte)0x8C}, 11);
//        correctReps.put(new byte[]{0x01,(byte)0x90}, 12);
//        correctReps.put(new byte[]{0x01,(byte)0x94}, 13);
//        correctReps.put(new byte[]{0x01,(byte)0x98}, 14);
//        correctReps.put(new byte[]{0x01,(byte)0x9C}, 15);
//        correctReps.put(new byte[]{0x01,(byte)0xA0}, 16);
//        correctReps.put(new byte[]{0x01,(byte)0xA2}, 17);
//
//        for(byte[] bytees:correctReps.keySet()){
//            SparseBitIndex index = SparseBitIndex.wrap(bytees,0,bytees.length);
//            Assert.assertTrue("Incorrect bytes with index "+ index,index.isSet(correctReps.get(bytees)));
//            Assert.assertEquals(1,index.cardinality());
//        }
//    }
//
//    @Test
//    public void testEncodesCorrectlyHandCheck() throws Exception {
//        //makes sure it lines up to the hand constructed entities
//        Map<Integer,byte[]> correctReps = Maps.newHashMap();
//        correctReps.put(1,new byte[]{0x04});
//        correctReps.put(2,new byte[]{0x02,(byte)0x80});
//        correctReps.put(3,new byte[]{0x02,(byte)0xC0});
//        correctReps.put(4,new byte[]{0x03,(byte)0x80});
//        correctReps.put(5,new byte[]{0x03,(byte)0xA0});
//        correctReps.put(6,new byte[]{0x03,(byte)0xC0});
//        correctReps.put(7,new byte[]{0x03,(byte)0xE0});
//        correctReps.put(8,new byte[]{0x01,(byte)0x80});
//        correctReps.put(9,new byte[]{0x01,(byte)0x84});
//        correctReps.put(10,new byte[]{0x01,(byte)0x88});
//        correctReps.put(11,new byte[]{0x01,(byte)0x8C});
//        correctReps.put(12,new byte[]{0x01,(byte)0x90});
//        correctReps.put(13,new byte[]{0x01,(byte)0x94});
//        correctReps.put(14,new byte[]{0x01,(byte)0x98});
//        correctReps.put(15,new byte[]{0x01,(byte)0x9C});
//        correctReps.put(16,new byte[]{0x01,(byte)0xA0});
//        correctReps.put(17,new byte[]{0x01,(byte)0xA2});
//
//        for(int val:correctReps.keySet()){
//            BitSet test = new BitSet();
//            test.set(val);
//
//            byte[] actual = new SparseBitIndex(test,new BitSet()).encode();
//            Assert.assertArrayEquals("Incorrect encoding for BitSet "+ test,correctReps.get(val),actual);
//        }
//    }


}
