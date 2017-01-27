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

package com.splicemachine.storage.index;


import com.carrotsearch.hppc.BitSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

/**
 * @author Scott Fines
 *         Created on: 7/8/13
 */
@RunWith(Parameterized.class)
public class SparseBitIndexTest {

    @Parameterized.Parameters
    public static Collection<Object[]> parameters(){
        List<Object[]> ds = new ArrayList<>();
        ds.add(splice816Data());
        ds.add(db5669Data());
        ds.add(new Object[]{new BitSet(),new BitSet(),new BitSet(),new BitSet()});

        BitSet nnCols = new BitSet();
        BitSet allP2 = new BitSet();
        BitSet p2Threes = new BitSet();
        BitSet threeFive = new BitSet();
        BitSet randoms = new BitSet();
        Random random = new Random(0L);
        BitSet others = new BitSet();
        int s = 1<<16;
        for(int i=1;i<s;i<<=1){
            nnCols.clear();
            nnCols.set(i);
            allP2.set(i);
            p2Threes.set(i*3);
            threeFive.set(i*3+i*5);

            int p;
            do{
                p = random.nextInt(i);
            }while(randoms.get(p));
            randoms.set(p);
            //powers of 2
            ds.add(new Object[]{nnCols,others,others,others});
            //all powers of 2
            ds.add(new Object[]{allP2,others,others,others});
            //divide p2 by 3
            ds.add(new Object[]{p2Threes,others,others,others});
            ds.add(new Object[]{threeFive,others,others,others});
        }

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
    public void testSparseEncodesAndDecodesProperly() throws Exception{
        BitIndex bi = SparseBitIndex.create(nnCols,scalars,doubles,floats);
        byte[] d = bi.encode();
        BitIndex nbi = SparseBitIndex.wrap(d,0,d.length);
        Assert.assertEquals("Incorrect serialization/deserialization!",bi,nbi);
    }

    private static Object[] splice816Data(){
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

        return new Object[]{nnCols,scalars,new BitSet(),new BitSet()};
    }

    private static Object[] db5669Data(){
        /*
         * Regression setting for db5669
         */
        BitSet nnCols = new BitSet();
        nnCols.set(128);

        return new Object[]{nnCols,new BitSet(),new BitSet(),new BitSet()};
    }

}
