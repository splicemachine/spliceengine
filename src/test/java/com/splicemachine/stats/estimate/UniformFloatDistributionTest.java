package com.splicemachine.stats.estimate;

import com.splicemachine.stats.FloatColumnStatistics;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.collector.ColumnStatsCollectors;
import com.splicemachine.stats.collector.FloatColumnStatsCollector;
import com.splicemachine.stats.frequency.FloatFrequencyCounter;
import com.splicemachine.stats.frequency.FloatFrequentElements;
import com.splicemachine.stats.frequency.FrequencyCounters;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Date: 6/25/15
 */
public class UniformFloatDistributionTest{

    @Test
    public void testGetPositiveCountForNegativeStartValues() throws Exception{
        FloatColumnStatsCollector col =ColumnStatsCollectors.floatCollector(0,14,5);
        for(int i=0;i<14;i++){
            col.update(0);
            col.update(1);
            col.update(-1);
            col.update(-Float.MAX_VALUE);
            col.update(Float.MAX_VALUE);
        }

        FloatColumnStatistics lcs = col.build();
        FloatDistribution distribution = new UniformFloatDistribution(lcs);

        long l=distribution.rangeSelectivity(-Float.MAX_VALUE,0,false,false);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",14,l);

        l=distribution.rangeSelectivity(-Float.MAX_VALUE,0,true,false);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",28,l);

        l=distribution.rangeSelectivity(-Float.MAX_VALUE,0,true,true);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",3*14l,l);
    }

    @Test
    public void distributionWorksWithFrequentElements() throws Exception {
 
    	FloatFrequencyCounter counter = FrequencyCounters.floatCounter(4);

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
        
		FloatFrequentElements fe = (FloatFrequentElements)counter.frequentElements(4);

        FloatColumnStatistics colStats = new FloatColumnStatistics(0,
            CardinalityEstimators.hyperLogLogFloat(4),
            fe,
            101,
            104,
            200,
            12,
            0,
            2);

        UniformFloatDistribution dist = new UniformFloatDistribution(colStats);

        Assert.assertEquals(2, dist.selectivity(101)); // return min of 2, not actual 1
        Assert.assertEquals(2, dist.selectivity(102));
        Assert.assertEquals(3, dist.selectivity(103));
        Assert.assertEquals(4, dist.selectivity(104));
        Assert.assertEquals(0, dist.selectivity(105));
    }
	
    @Test
    public void testDistributionWorksWithSingleElement() throws Exception{
        //the test is to make sure that we can create the entity without it breaking
        FloatColumnStatistics scs = new FloatColumnStatistics(0,
                CardinalityEstimators.hyperLogLogFloat(4),
                FrequencyCounters.floatCounter(4).frequentElements(4),
                1,
                1,
                2,
                12,
                0,
                3);

        UniformFloatDistribution dist=new UniformFloatDistribution(scs);
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
        FloatColumnStatistics scs = new FloatColumnStatistics(0,
                CardinalityEstimators.hyperLogLogFloat(4),
                FrequencyCounters.floatCounter(4).frequentElements(4),
                0,
                0,
                0,
                0,
                0,
                0);

        UniformFloatDistribution dist=new UniformFloatDistribution(scs);
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