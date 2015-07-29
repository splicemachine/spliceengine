package com.splicemachine.stats.estimate;

import com.splicemachine.stats.DoubleColumnStatistics;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.frequency.DoubleFrequencyCounter;
import com.splicemachine.stats.frequency.DoubleFrequentElements;
import com.splicemachine.stats.frequency.FrequencyCounters;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Date: 6/25/15
 */
public class UniformDoubleDistributionTest{

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
        
		DoubleFrequentElements fe = (DoubleFrequentElements)counter.frequentElements(4);

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

        Assert.assertEquals(2, dist.selectivity(101)); // return min of 2, not actual 1
        Assert.assertEquals(2, dist.selectivity(102));
        Assert.assertEquals(3, dist.selectivity(103));
        Assert.assertEquals(4, dist.selectivity(104));
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
}