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

import com.carrotsearch.hppc.FloatArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.splicemachine.stats.LongColumnStatistics;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.collector.ColumnStatsCollectors;
import com.splicemachine.stats.collector.FloatColumnStatsCollector;
import com.splicemachine.stats.collector.LongColumnStatsCollector;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.LongFrequencyCounter;
import com.splicemachine.stats.frequency.LongFrequentElements;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Time;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * @author Scott Fines
 *         Date: 6/25/15
 */
public class UniformLongDistributionTest{

    @Test
    public void testCardinalityCorrectForTimeGeneratedFields() throws Exception{
        /*
         * Regression test for DB-3657, just to be safe.
         */
        LongColumnStatsCollector col = ColumnStatsCollectors.longCollector(0,14,5);

        Calendar c = new GregorianCalendar();
        c.set(Calendar.HOUR,15);
        c.set(Calendar.MINUTE,10);
        for(int i=1;i<=4;i++){
            c.set(Calendar.SECOND,i);
            Time t = new Time(c.getTimeInMillis());
            col.update(t.getTime());
        }
        Assert.assertEquals("Cardinality is incorrect!",4,col.build().cardinality());
    }

    @Test
    public void testCorrectStatisticsWhenFilledOnlyWithNulls() throws Exception{
        LongColumnStatsCollector col = ColumnStatsCollectors.longCollector(0,14,5);

        for(int i=0;i<100;i++){
            col.updateNull();
        }

        LongColumnStatistics stats=col.build();
        Assert.assertEquals("incorrect non-null count!",0l,stats.nonNullCount());
        Assert.assertEquals("incorrect null count!",100,stats.nullCount());
        Assert.assertEquals("incorrect null fraction!",1f,stats.nullFraction(),1e-6f);
        Assert.assertEquals("incorrect cardinality!",0l,stats.cardinality());
        Assert.assertEquals("incorrect max!",0l,stats.min());
        Assert.assertEquals("incorrect min!",0l,stats.max());
    }

    @Test
    public void testSelectivityRemainsBounded() throws Exception{
        /*
         * The idea here is to ensure that the selectivity estimates that we provide don't violate
         * the invariants of falling within the range [0,totalCount()).
         */
        LongColumnStatsCollector col = ColumnStatsCollectors.longCollector(0,14,5);
        LongArrayList values = new LongArrayList(1000);
        for(int i=0;i<100;i++){
            long il = 1l<<i;

            long v = il;
            col.update(v);
            values.add(v);
            v = -il;
            col.update(v);
            values.add(v);

        }

        values.add(0l);
        values.add(Long.MAX_VALUE);
        values.add(Long.MIN_VALUE);

        LongDistribution distribution=(LongDistribution)col.build().getDistribution();
        long[] v = values.toArray();
        Arrays.sort(v);
        for(int i=0;i<v.length;i++){
            long mi = v[i];
            long sel = distribution.selectivity(mi);

            Assert.assertTrue("negative selectivity!",sel>=0);
            Assert.assertTrue("overlarge selectivity!",sel<=v.length);
            Assert.assertTrue("overlarge selectivity!",sel<=distribution.totalCount());

            for(int j=i+1;j<v.length;j++){
                long ma = v[j];

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

    @Test
    public void testGetPositiveCountForNegativeStartValues() throws Exception{
        LongColumnStatsCollector col =ColumnStatsCollectors.longCollector(0,14,5);
        for(int i=0;i<14;i++){
            col.update(0l);
            col.update(1l);
            col.update(-1l);
            col.update(Long.MIN_VALUE);
            col.update(Long.MAX_VALUE);
        }

        LongColumnStatistics lcs = col.build();
        LongDistribution distribution = new UniformLongDistribution(lcs);

        long l=distribution.rangeSelectivity(-9223372036854775808l,0l,false,false);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",14,l);

        l=distribution.rangeSelectivity(-9223372036854775808l,0l,true,false);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",28,l);

        l=distribution.rangeSelectivity(-9223372036854775808l,0l,true,true);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",3*14l,l);
    }

    @Test
    public void distributionWorksWithFrequentElements() throws Exception {
 
    	LongFrequencyCounter counter = FrequencyCounters.longCounter(4);

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
        
		LongFrequentElements fe = (LongFrequentElements)counter.frequentElements(4);

        LongColumnStatistics colStats = new LongColumnStatistics(0,
            CardinalityEstimators.hyperLogLogLong(4),
            fe,
            101,
            104,
            200,
            12,
            0,
            2);

        UniformLongDistribution dist = new UniformLongDistribution(colStats);

        Assert.assertEquals(2, dist.selectivity(101)); // return min of 2, not actual 1
        Assert.assertEquals(2, dist.selectivity(102));
        Assert.assertEquals(3, dist.selectivity(103));
        Assert.assertEquals(4, dist.selectivity(104));
        Assert.assertEquals(0, dist.selectivity(105));
    }
	
    @Test
    public void testDistributionWorksWithSingleElement() throws Exception{
        //the test is to make sure that we can create the entity without it breaking
        LongColumnStatistics scs = new LongColumnStatistics(0,
                CardinalityEstimators.hyperLogLogLong(4),
                FrequencyCounters.longCounter(4).frequentElements(4),
                1,
                1,
                2,
                12,
                0,
                3);

        UniformLongDistribution dist=new UniformLongDistribution(scs);
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
        LongColumnStatistics scs = new LongColumnStatistics(0,
                CardinalityEstimators.hyperLogLogLong(4),
                FrequencyCounters.longCounter(4).frequentElements(4),
                0,
                0,
                0,
                0,
                0,
                0);

        UniformLongDistribution dist=new UniformLongDistribution(scs);
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
}