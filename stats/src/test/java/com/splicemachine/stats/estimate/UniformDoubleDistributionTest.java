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

package com.splicemachine.stats.estimate;

import com.carrotsearch.hppc.DoubleArrayList;
import com.splicemachine.stats.DoubleColumnStatistics;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.collector.ColumnStatsCollectors;
import com.splicemachine.stats.collector.DoubleColumnStatsCollector;
import com.splicemachine.stats.frequency.DoubleFrequencyCounter;
import com.splicemachine.stats.frequency.DoubleFrequentElements;
import com.splicemachine.stats.frequency.FrequencyCounters;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author Scott Fines
 *         Date: 6/25/15
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class UniformDoubleDistributionTest{

    @Test
    public void testSelectivityRemainsBounded() throws Exception{
        /*
         * The idea here is to ensure that the selectivity estimates that we provide don't violate
         * the invariants of falling within the range [0,totalCount()).
         */
        DoubleColumnStatsCollector col = ColumnStatsCollectors.doubleCollector(0,14,5);
        double[] v=loadData(col);

        DoubleDistribution distribution=(DoubleDistribution)col.build().getDistribution();
        distribution.rangeSelectivity(Double.NEGATIVE_INFINITY,Double.POSITIVE_INFINITY,true,true);
        for(int i=0;i<v.length;i++){
            double mi = v[i];
            long sel = distribution.selectivity(mi);

            Assert.assertTrue("negative selectivity!",sel>=0);
            Assert.assertTrue("overlarge selectivity!",sel<=v.length);
            Assert.assertTrue("overlarge selectivity!",sel<=distribution.totalCount());

            for(int j=i+1;j<v.length;j++){
                double ma = v[j];

                long rs=distribution.rangeSelectivity(mi,ma,true,true);
                Assert.assertTrue("negative selectivity: mi=<"+mi+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: mi=<"+mi+">, ma=<"+ma+">!:rs="+rs,rs<=v.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());

                rs=distribution.rangeSelectivity(mi,ma,true,false);
                Assert.assertTrue("negative selectivity: mi=<"+mi+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: mi=<"+mi+">, ma=<"+ma+">!:rs="+rs,rs<=v.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());

                rs=distribution.rangeSelectivity(mi,ma,false,true);
                Assert.assertTrue("negative selectivity: mi=<"+mi+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: mi=<"+mi+">, ma=<"+ma+">!:rs="+rs,rs<=v.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());

                rs=distribution.rangeSelectivity(mi,ma,false,false);
                Assert.assertTrue("negative selectivity: mi=<"+mi+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: mi=<"+mi+">, ma=<"+ma+">!:rs="+rs,rs<=v.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());
            }
        }
    }

    @Test(expected=ArithmeticException.class)
    public void testCannotGetSelectivityForNan() throws Exception{
        DoubleColumnStatsCollector col = ColumnStatsCollectors.doubleCollector(0,14,5);
        loadData(col);

        DoubleDistribution distribution=(DoubleDistribution)col.build().getDistribution();
        distribution.selectivity(Double.NaN);
    }

    @Test
    public void testCannotGetRangeSelectivityForNanStop() throws Exception{
        DoubleColumnStatsCollector col = ColumnStatsCollectors.doubleCollector(0,14,5);
        double[] v=loadData(col);

        DoubleDistribution distribution=(DoubleDistribution)col.build().getDistribution();
        for(int i=0;i<v.length;i++){
            double d = v[i];
            try{
                distribution.rangeSelectivity(d,Double.NaN,true,true);
                Assert.fail("Was able to perform selectivity for ["+d+",Nan]!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(d,Double.NaN,true,false);
                Assert.fail("Was able to perform selectivity for ["+d+",Nan)!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(d,Double.NaN,false,true);
                Assert.fail("Was able to perform selectivity for ("+d+",Nan]!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(d,Double.NaN,false,false);
                Assert.fail("Was able to perform selectivity for ("+d+",Nan)!");
            }catch(ArithmeticException ignored){}
        }
    }

    @Test
    public void testCannotGetRangeSelectivityForNanStart() throws Exception{
        DoubleColumnStatsCollector col = ColumnStatsCollectors.doubleCollector(0,14,5);
        double[] v=loadData(col);

        DoubleDistribution distribution=(DoubleDistribution)col.build().getDistribution();
        for(int i=0;i<v.length;i++){
            double d = v[i];
            try{
                distribution.rangeSelectivity(Double.NaN,d,true,true);
                Assert.fail("Was able to perform selectivity for [NaN,"+d+"]!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(Double.NaN,d,true,false);
                Assert.fail("Was able to perform selectivity for [NaN,"+d+")!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(Double.NaN,d,false,true);
                Assert.fail("Was able to perform selectivity for (NaN,"+d+"]!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(Double.NaN,d,false,false);
                Assert.fail("Was able to perform selectivity for (NaN,"+d+")!");
            }catch(ArithmeticException ignored){}
        }
    }

    @Test
    public void testPositiveInfinitySelectivityCorrect(){
        DoubleColumnStatsCollector col = ColumnStatsCollectors.doubleCollector(0,14,5);
        double[] v=loadData(col);

        DoubleDistribution distribution=(DoubleDistribution)col.build().getDistribution();
        double mi = Double.POSITIVE_INFINITY;
        long s = distribution.selectivity(mi);
        Assert.assertEquals("Did not return 0 for nonexistent numbers!",0l,s);
        double max = distribution.max();
        for(int j=0;j<v.length;j++){
            double ma = v[j];
            long rs=distribution.rangeSelectivity(ma,mi,true,true);
            long cs = distribution.rangeSelectivity(ma,max,true,true);
            Assert.assertEquals("selectivity of ["+ma+",Inf] incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(ma,mi,true,false);
            cs = distribution.rangeSelectivity(ma,max,true,true);
            Assert.assertEquals("selectivity of ["+ma+",Inf) incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(ma,mi,false,true);
            cs = distribution.rangeSelectivity(ma,max,false,true);
            Assert.assertEquals("selectivity of ("+ma+",Inf] incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(ma,mi,false,false);
            cs = distribution.rangeSelectivity(ma,max,false,true);
            Assert.assertEquals("selectivity of ("+ma+",Inf) incorrect!",cs,rs);
        }
    }

    @Test
    public void testNegativeInfinitySelectivityCorrect(){
        DoubleColumnStatsCollector col = ColumnStatsCollectors.doubleCollector(0,14,5);
        double[] v=loadData(col);

        DoubleDistribution distribution=(DoubleDistribution)col.build().getDistribution();
        double mi = Double.NEGATIVE_INFINITY;
        long s = distribution.selectivity(mi);
        Assert.assertEquals("Did not return 0 for nonexistent numbers!",0l,s);
        double min = distribution.min();
        for(int j=0;j<v.length;j++){
            double ma = v[j];
            long rs=distribution.rangeSelectivity(mi,ma,true,true);
            long cs = distribution.rangeSelectivity(min,ma,true,true);
            Assert.assertEquals("selectivity of [-Inf,ma] incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(mi,ma,true,false);
            cs = distribution.rangeSelectivity(min,ma,true,false);
            Assert.assertEquals("selectivity of [-Inf,ma] incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(mi,ma,false,true);
            cs = distribution.rangeSelectivity(min,ma,true,true);
            Assert.assertEquals("selectivity of [-Inf,ma] incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(mi,ma,false,false);
            cs = distribution.rangeSelectivity(min,ma,true,false);
            Assert.assertEquals("selectivity of [-Inf,ma] incorrect!",cs,rs);
        }
    }


    @Test
    public void testGetPositiveCountForNegativeStartValues() throws Exception{
        DoubleColumnStatsCollector col =ColumnStatsCollectors.doubleCollector(0,14,5);
        for(int i=0;i<14;i++){
            col.update(0);
            col.update(1);
            col.update(-1);
            col.update(-Double.MAX_VALUE);
            col.update(Double.MAX_VALUE);
        }

        DoubleColumnStatistics lcs = col.build();
        DoubleDistribution distribution = new UniformDoubleDistribution(lcs);

        long l=distribution.rangeSelectivity(-Double.MAX_VALUE,0,false,false);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",14,l);

        l=distribution.rangeSelectivity(-Double.MAX_VALUE,0,true,false);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",28,l);

        l=distribution.rangeSelectivity(-Double.MAX_VALUE,0,true,true);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",3*14l,l);
    }

    @Test
    public void distributionWorksWithFrequentElements() throws Exception {
 
    	DoubleFrequencyCounter counter = FrequencyCounters.doubleCounter(4);

        // Values repeated on purpose
        counter.update(101);
        counter.update(102);
        counter.update(102);
        counter.update(103);
        counter.update(103);
        counter.update(103);
        counter.update(104);
        counter.update(104);
        counter.update(104);
        counter.update(104);
        
		DoubleFrequentElements fe =counter.frequentElements(4);

		DoubleColumnStatistics colStats = new DoubleColumnStatistics(0,
            CardinalityEstimators.hyperLogLogDouble(4),
            fe,
            101,
            104,
            200,
            12,
            0,
            2);

        UniformDoubleDistribution dist = new UniformDoubleDistribution(colStats);

        Assert.assertEquals(2,dist.selectivity(101)); // return min of 2, not actual 1
        Assert.assertEquals(2, dist.selectivity(102));
        Assert.assertEquals(3, dist.selectivity(103));
        Assert.assertEquals(4,dist.selectivity(104));
        Assert.assertEquals(0, dist.selectivity(105));
    }
	
    @Test
    public void testDistributionWorksWithSingleElement() throws Exception{
        //the test is to make sure that we can create the entity without it breaking
        DoubleColumnStatistics scs = new DoubleColumnStatistics(0,
                CardinalityEstimators.hyperLogLogDouble(4),
                FrequencyCounters.doubleCounter(4).frequentElements(4),
                1,
                1,
                2,
                12,
                0,
                3);

        UniformDoubleDistribution dist=new UniformDoubleDistribution(scs);
        /*
         * We need to make sure of the following things:
         *
         * 1. values == min or max return the correct count
         * 2. Values != min return 0
         * 3. Range estimates which include the min return minCount
         * 4. Range estimates which do not include the min return 0
         */
        Assert.assertEquals(scs.minCount(),dist.selectivity(scs.min()));
        Assert.assertEquals(0l,dist.selectivity((scs.min()+1)));

        Assert.assertEquals(scs.minCount(),dist.rangeSelectivity(scs.min(),(scs.min()+1),true,true));
        Assert.assertEquals(0,dist.rangeSelectivity(scs.min(),(scs.min()+1),false,true));
    }

    @Test
    public void emptyDistributionReturnsZeroForAllEstimates() throws Exception{
        //the test is to make sure that we can create the entity without it breaking
        DoubleColumnStatistics scs = new DoubleColumnStatistics(0,
                CardinalityEstimators.hyperLogLogDouble(4),
                FrequencyCounters.doubleCounter(4).frequentElements(4),
                0,
                0,
                0,
                0,
                0,
                0);

        UniformDoubleDistribution dist=new UniformDoubleDistribution(scs);
        /*
         * We need to make sure we return 0 in the following scenarios:
         *
         * 1. values == scs.min()
         * 2. Values != min return 0
         * 3. Range estimates which include scs.min()
         * 4. Range estimates which do not include the min return 0
         */
        Assert.assertEquals(0,dist.selectivity(scs.min()));
        Assert.assertEquals(0l,dist.selectivity((scs.min()+1)));

        Assert.assertEquals(0,dist.rangeSelectivity(scs.min(),(scs.min()+1),true,true));
        Assert.assertEquals(0,dist.rangeSelectivity(scs.min(),(scs.min()+1),false,true));
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private double[] loadData(DoubleColumnStatsCollector col){
        DoubleArrayList values = new DoubleArrayList(1000);
        for(int i=0;i<100;i++){
            long il = 1l<<i;

            double v = il;
            col.update(v);
            values.add(v);
            v = -il;
            col.update(v);
            values.add(v);
            v = 1d/il;
            col.update(v);
            values.add(v);
            v = -1d/il;
            col.update(v);
            values.add(v);

            v = 3d*il;
            col.update(v);
            values.add(v);
            v = -3d*il;
            col.update(v);
            values.add(v);
            v = 3d/il;
            col.update(v);
            values.add(v);
            v = -3d/il;
            col.update(v);
            values.add(v);
        }

        values.add(-0d);
        values.add(0d);
        values.add(Double.MAX_VALUE);
        values.add(-Double.MAX_VALUE);
        values.add(Double.MIN_VALUE);
        values.add(-Double.MIN_VALUE);
        double[] doubles=values.toArray();
        Arrays.sort(doubles);
        return doubles;
    }
}