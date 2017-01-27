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

import com.splicemachine.primitives.Bytes;
import com.splicemachine.testutil.ParallelTheoryRunner;
import org.junit.Assert;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Test which covers a systematic subset possible BigDecimals, to determine if they encode and sort
 * properly with respect to one another.
 *
 * For efficiency, this uses a Parallel theory runner, but it is still considering a large number of states,
 * so expect it to take a little while.
 *
 * @author Scott Fines
 *         Date: 6/23/15
 */
//@RunWith(Theories.class)
@RunWith(ParallelTheoryRunner.class)
public class ExhaustiveBigDecimalEncodingTest{

    private static final BigDecimal TWO=BigDecimal.valueOf(2);

    private static final BigDecimal THREE=BigDecimal.valueOf(3);

    private static final BigDecimal FIVE=BigDecimal.valueOf(5);

    private static final BigDecimal SEVEN=BigDecimal.valueOf(7);

    /*
         * We use powers and multipliers of primes here so that we get good coverage density in
         * the very small numbers (which tend to be more common than the larger numbers). Of course,
         * as the numbers get larger, these powers and combinations get further and further away from
         * one another, but we still get a pretty consistent coverage of the entire numerical space just
         * by considering positive and negative combinations of these numbers.
         */

    @DataPoints
    public static BigDecimal[] powersOf2(){
        List<BigDecimal> dataPoints = new ArrayList<>(100);
        BigDecimal l = BigDecimal.ONE;
        BigDecimal d = BigDecimal.ONE;
        for(int i=0;i<20;i++){ //the first 100 powers of 2
            BigDecimal n = l;
            dataPoints.add(n);
            n = n.negate();
            dataPoints.add(n);
            n = n.multiply(THREE);
            dataPoints.add(n);
            n = n.negate();
            dataPoints.add(n);
            n = n.multiply(FIVE);
            dataPoints.add(n);
            n = n.negate();
            dataPoints.add(n);
            n = n.multiply(SEVEN);
            dataPoints.add(n);
            n = n.negate();
            dataPoints.add(n);

            l = l.multiply(TWO);

            //add some fractional powers
            n = d;
            dataPoints.add(n);
            n = n.negate();
            dataPoints.add(n);
            n = n.divide(THREE,MathContext.DECIMAL128);
            dataPoints.add(n);
            n = n.negate();
            dataPoints.add(n);
            n = n.divide(FIVE,MathContext.DECIMAL128);
            dataPoints.add(n);
            n = n.negate();
            dataPoints.add(n);
            n = n.divide(SEVEN,MathContext.DECIMAL128);
            dataPoints.add(n);
            n = n.negate();
            dataPoints.add(n);
            d = d.divide(TWO,MathContext.DECIMAL128);
        }

        BigDecimal[] dp = new BigDecimal[dataPoints.size()];
        dataPoints.toArray(dp);
        return dp;
    }



    @DataPoints public static BigDecimal[] knownProblemPoints(){
        return new BigDecimal[]{
                BigDecimal.valueOf(-9208636019293794487l), //DB-3421
                BigDecimal.valueOf(-9169196554323565708l), //DB-3421
                BigDecimal.valueOf(-9219236770852362184l), //contains 0 bytes internally
                BigDecimal.valueOf(Integer.MIN_VALUE),
                BigDecimal.valueOf(Integer.MAX_VALUE),
                BigDecimal.valueOf(Long.MIN_VALUE),
                BigDecimal.valueOf(Long.MAX_VALUE),
                BigDecimal.valueOf(Long.MAX_VALUE | Long.MIN_VALUE), //all 1s
                BigDecimal.valueOf(Integer.MAX_VALUE| Integer.MIN_VALUE), //all 1s, but only in the integer space
                BigDecimal.valueOf(18278),
                BigDecimal.ZERO,
                BigDecimal.ONE,
                BigDecimal.TEN,
        };
    }

    @Theory
    public void encodedDataSortsCorrectlyAscending(BigDecimal e1,BigDecimal e2){
        byte[] d1 = BigDecimalEncoding.toBytes(e1,false);
        byte[] d2 = BigDecimalEncoding.toBytes(e2,false);
        int compare = Bytes.BASE_COMPARATOR.compare(d1,d2);
        int correctCompare = e1.compareTo(e2);
        if(correctCompare<0){
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly!",compare<0);
        }else if(correctCompare>0){
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly!",compare>0);
        }else
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly!",compare==0);
    }

    @Theory
    public void encodedDataSortsCorrectlyDescending(BigDecimal e1,BigDecimal e2){
        byte[] d1 = BigDecimalEncoding.toBytes(e1,true);
        byte[] d2 = BigDecimalEncoding.toBytes(e2,true);
        int compare =Bytes.BASE_COMPARATOR.compare(d1,d2);
        int correctCompare = e1.compareTo(e2);
        if(correctCompare<0){
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly!",compare>0);
        }else if(correctCompare>0){
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly!",compare<0);
        }else
            Assert.assertTrue("Elements <"+e1+">,<"+e2+"> do not sort correctly!",compare==0);
    }

    @Theory
    public void dataEncodesAndDecodesCorrectlyAscending(BigDecimal e){
        byte[] data = BigDecimalEncoding.toBytes(e,false);
        BigDecimal decoded = BigDecimalEncoding.toBigDecimal(data,false);
        Assert.assertTrue("Element "+e+" did not encode/decode properly!Correct:"+e+",Actual:"+decoded,e.compareTo(decoded)==0);
    }

    @Theory
    public void dataEncodesAndDecodesCorrectlyDescending(BigDecimal e){
        byte[] data = BigDecimalEncoding.toBytes(e,true);
        BigDecimal decoded = BigDecimalEncoding.toBigDecimal(data,true);
        Assert.assertTrue("Element "+e+" did not encode/decode properly!Correct:"+e+",Actual:"+decoded,e.compareTo(decoded)==0);
    }

    private static final byte[] nullBytes = new byte[]{0x00};
    @Theory
    public void nullSortsFirstAscending(BigDecimal e){
        byte[] data = BigDecimalEncoding.toBytes(e,false);
        Assert.assertTrue("Element <"+ e+"> does not compare correctly with null",Bytes.BASE_COMPARATOR.compare(nullBytes,data)<0);
    }

    @Theory
    public void nullSortsFirstDescending(BigDecimal e){
        byte[] data = BigDecimalEncoding.toBytes(e,true);
        Assert.assertTrue("Element <"+ e+"> does not compare correctly with null",Bytes.BASE_COMPARATOR.compare(nullBytes,data)<0);
    }

}
