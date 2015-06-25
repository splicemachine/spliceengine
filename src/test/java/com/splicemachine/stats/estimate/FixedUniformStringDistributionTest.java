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
    public void distributionWorksWithASingleElement() throws Exception{
        FrequencyCounter<? super String> counter=FrequencyCounters.counter(ComparableComparator.<String>newComparator(),1,16);
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
        FrequencyCounter<? super String> counter=FrequencyCounters.counter(ComparableComparator.<String>newComparator(),1,16);
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