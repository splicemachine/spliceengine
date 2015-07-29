package com.splicemachine.stats.estimate;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.ComparableColumnStatistics;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.frequency.FrequencyCounter;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.utils.ComparableComparator;

import org.junit.Assert;
import org.junit.Test;

/**
 * Specific tests about the UniformStringDistribution
 *
 * @author Scott Fines
 *         Date: 6/25/15
 */
public class FixedUniformStringDistributionTest{

    @Test
    public void distributionWorksWithFrequentElements() throws Exception {
    	// This test was added to cover issue in DB-3608
    	
        FrequencyCounter<? super String> counter =
			FrequencyCounters.counter(ComparableComparator.<String>newComparator(), 16, 1);

        // Values repeated on purpose
        counter.update("A");
        counter.update("B");
        counter.update("B");
        counter.update("C");
        counter.update("C");
        counter.update("C");
        counter.update("D");
        counter.update("D");
        counter.update("D");
        counter.update("D");
        
        @SuppressWarnings("unchecked")
		FrequentElements<String> fe = (FrequentElements<String>)counter.frequentElements(5);

        // TODO: consider using ComparableColumn for stats integrity instead of
        // having to construct everything perfectly with this constructor.
        ColumnStatistics<String> colStats = new ComparableColumnStatistics<>(0,
            CardinalityEstimators.hyperLogLogString(4),
            fe,
            "A",
            "D",
            200,
            12,
            0,
            2,
            UniformStringDistribution.factory(5));

        UniformStringDistribution dist = new UniformStringDistribution(colStats, 5);

        Assert.assertEquals(2, dist.selectivity("A")); // return min of 2, not actual 1
        Assert.assertEquals(2, dist.selectivity("B"));
        Assert.assertEquals(3, dist.selectivity("C"));
        Assert.assertEquals(4, dist.selectivity("D"));
        Assert.assertEquals(0, dist.selectivity("Z"));
    }
	
	@Test
    public void distributionWorksWithASingleElement() throws Exception{
        FrequencyCounter<? super String> counter=FrequencyCounters.counter(ComparableComparator.<String>newComparator(),16,1);
        @SuppressWarnings("unchecked")
		FrequentElements<String> fe =(FrequentElements<String>)counter.frequentElements(1);
        ColumnStatistics<String> colStats = new ComparableColumnStatistics<>(0,
                CardinalityEstimators.hyperLogLogString(4),
                fe,
                "hello",
                "hello",
                200,
                12,
                0,
                12,
                UniformStringDistribution.factory(5));

        //make sure the creation goes well
        UniformStringDistribution dist=new UniformStringDistribution(colStats,5);

        /*
         * We need to make sure of the following things:
         *
         * 1. values == min or max return the correct count
         * 2. Values != min return 0
         * 3. Range estimates which include the min return minCount
         * 4. Range estimates which do not include the min return 0
         */
        Assert.assertEquals(colStats.minCount(),dist.selectivity(colStats.minValue()));
        Assert.assertEquals(0l,dist.selectivity("hello1"));

        Assert.assertEquals(colStats.minCount(),dist.rangeSelectivity(colStats.minValue(),"hello1",true,true));
        Assert.assertEquals(0,dist.rangeSelectivity(colStats.minValue(),"hello2",false,true));
    }

    @Test
    public void distributionWorksForNoElements() throws Exception{
        FrequencyCounter<? super String> counter=FrequencyCounters.counter(ComparableComparator.<String>newComparator(),16,1);
        @SuppressWarnings("unchecked")
		FrequentElements<String> fe =(FrequentElements<String>)counter.frequentElements(1);
        ColumnStatistics<String> colStats = new ComparableColumnStatistics<>(0,
                CardinalityEstimators.hyperLogLogString(4),
                fe,
                null,
                null,
                0,
                0,
                0,
                0,
                UniformStringDistribution.factory(5));

        //make sure the creation goes well
        UniformStringDistribution dist=new UniformStringDistribution(colStats,5);

        /*
         * We need to make sure of the following things:
         *
         * 1. values == min or max return the correct count
         * 2. Values != min return 0
         * 3. Range estimates which include the min return minCount
         * 4. Range estimates which do not include the min return 0
         */
        Assert.assertEquals(0l,dist.selectivity("hello"));
        Assert.assertEquals(0l,dist.selectivity("hello1"));

        Assert.assertEquals(0l,dist.rangeSelectivity("hello","hello1",true,true));
        Assert.assertEquals(0,dist.rangeSelectivity("hello","hello2",false,true));
    }
}