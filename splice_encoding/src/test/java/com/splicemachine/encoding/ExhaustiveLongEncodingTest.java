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

package com.splicemachine.encoding;

import com.carrotsearch.hppc.LongArrayList;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.testutil.ParallelTheoryRunner;
import org.junit.Assert;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/**
 * Test which covers a systematic subset of all possible longs, to determine if they encode and sort
 * properly with respect to one another.
 *
 * For efficiency, this uses a Parallel theory runner, but it is still considering a large number of states,
 * so expect it to take a little while.
 *
 * @author Scott Fines
 *         Date: 6/23/15
 */
@RunWith(ParallelTheoryRunner.class)
public class ExhaustiveLongEncodingTest{

    /*
     * We use powers and multipliers of primes here so that we get good coverage density in
     * the very small numbers (which tend to be more common than the larger numbers). Of course,
     * as the numbers get larger, these powers and combinations get further and further away from
     * one another, but we still get a pretty consistent coverage of the entire numerical space just
     * by considering positive and negative combinations of these numbers.
     */
    @DataPoints public static long[] powersOf2(){
        LongArrayList dataPoints = new LongArrayList(100);
        long l = 1l;
        while(l>0){
            dataPoints.add(l);
            dataPoints.add(-l);
            dataPoints.add(3*l);
            dataPoints.add(-3*l);
            dataPoints.add(5*l);
            dataPoints.add(-5*l);
            dataPoints.add(7*l);
            dataPoints.add(-7*l);
            l<<=1;
        }

        return dataPoints.toArray();
    }

//    @DataPoints public static long[] powersOf3(){
//        LongArrayList dataPoints = new LongArrayList(100);
//        long l = 1l;
//        while(l>0){
//            dataPoints.add(l);
//            dataPoints.add(-l);
//            dataPoints.add(5*l);
//            dataPoints.add(-5*l);
//            dataPoints.add(7*l);
//            dataPoints.add(-7*l);
//            l*=3;
//        }
//
//        return dataPoints.toArray();
//    }

//    @DataPoints public static long[] powersOf5(){
//        LongArrayList dataPoints = new LongArrayList(100);
//        long l = 1l;
//        while(l>0){
//            dataPoints.add(l);
//            dataPoints.add(-l);
//            dataPoints.add(7*l);
//            dataPoints.add(-7*l);
//            l*=5;
//        }
//
//        return dataPoints.toArray();
//    }

    @DataPoints public static long[] powersOf7(){
        LongArrayList dataPoints = new LongArrayList(100);
        long l = 1l;
        while(l>0){
            dataPoints.add(l);
            dataPoints.add(-l);
            l*=7;
        }

        return dataPoints.toArray();
    }

    @DataPoints public static long[] knownProblemPoints(){
        return new long[]{
                -9208636019293794487l, //DB-3421
                -9169196554323565708l, //DB-3421
                -9219236770852362184l, //contains 0 bytes internally
                Integer.MIN_VALUE,
                Integer.MAX_VALUE,
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE | Long.MIN_VALUE, //all 1s
                Integer.MAX_VALUE| Integer.MIN_VALUE, //all 1s, but only in the integer space
                18278
        };
    }

    @Theory
    public void encodedDataSortsCorrectlyAscending(long e1,long e2){
        byte[] d1 = ScalarEncoding.writeLong(e1,false);
        byte[] d2 = ScalarEncoding.writeLong(e2,false);
        int compare = Bytes.BASE_COMPARATOR.compare(d1,d2);
        if(e1<e2){
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly",compare<0);
        }else if(e2<e1){
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly",compare>0);
        }else
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly",compare==0);
    }

    @Theory
    public void encodedDataSortsCorrectlyDescending(long e1,long e2){
        byte[] d1 = ScalarEncoding.writeLong(e1,true);
        byte[] d2 = ScalarEncoding.writeLong(e2,true);
        int compare =Bytes.BASE_COMPARATOR.compare(d1,d2);
        if(e1<e2){
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly",compare>0);
        }else if(e2<e1){
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly",compare<0);
        }else
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly",compare==0);

    }

    @Theory
    public void dataEncodesAndDecodesCorrectlyAscending(long e){
        byte[] data = ScalarEncoding.writeLong(e,false);
        long decoded = ScalarEncoding.readLong(data,false);
        Assert.assertEquals("Element "+e+" did not encode/decode properly!",e,decoded);
    }

    @Theory
    public void dataEncodesAndDecodesCorrectlyDescending(long e){
        byte[] data = ScalarEncoding.writeLong(e,true);
        long decoded = ScalarEncoding.readLong(data,true);
        Assert.assertEquals("Element "+e+" did not encode/decode properly!",e,decoded);
    }

    private static final byte[] nullBytes = new byte[]{0x00};
    @Theory
    public void nullSortsFirstAscending(long e){
        byte[] data = ScalarEncoding.writeLong(e,false);
        Assert.assertTrue("Element <"+ e+"> does not compare correctly with null",Bytes.BASE_COMPARATOR.compare(nullBytes,data)<0);
    }

    @Theory
    public void nullSortsFirstDescending(long e){
        byte[] data = ScalarEncoding.writeLong(e,true);
        Assert.assertTrue("Element <"+ e+"> does not compare correctly with null",Bytes.BASE_COMPARATOR.compare(nullBytes,data)<0);
    }
}
