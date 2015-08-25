package com.splicemachine.stats.estimate;

import com.splicemachine.stats.LongColumnStatistics;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.collector.ColumnStatsCollectors;
import com.splicemachine.stats.collector.LongColumnStatsCollector;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.LongFrequencyCounter;
import com.splicemachine.stats.frequency.LongFrequentElements;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Date: 6/25/15
 */
public class UniformLongDistributionTest{

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