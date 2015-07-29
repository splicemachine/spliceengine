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

import java.math.BigDecimal;

/**
 * @author Scott Fines
 *         Date: 6/25/15
 */
public class UniformDecimalDistributionTest{

    @Test
    public void distributionWorksWithFrequentElements() throws Exception {
 
        FrequencyCounter<? super BigDecimal> counter=FrequencyCounters.counter(ComparableComparator.<BigDecimal>newComparator(),4);

        // Values repeated on purpose
        counter.update(new BigDecimal(101));
        counter.update(new BigDecimal(102));
        counter.update(new BigDecimal(102));
        counter.update(new BigDecimal(103));
        counter.update(new BigDecimal(103));
        counter.update(new BigDecimal(103));
        counter.update(new BigDecimal(104));
        counter.update(new BigDecimal(104));
        counter.update(new BigDecimal(104));
        counter.update(new BigDecimal(104));
        
        FrequentElements<BigDecimal> fe = (FrequentElements<BigDecimal>)counter.frequentElements(4);

        ColumnStatistics<BigDecimal> colStats = new ComparableColumnStatistics<>(0,
            CardinalityEstimators.hyperLogLogBigDecimal(4),
            fe,
            new BigDecimal(101),
            new BigDecimal(104),
            200,
            12,
            0,
            2,
            new DistributionFactory<BigDecimal>(){
	            @Override
	            public Distribution<BigDecimal> newDistribution(ColumnStatistics<BigDecimal> statistics){
	                return new UniformDecimalDistribution(statistics);
	            }
        	});

        UniformDecimalDistribution dist=new UniformDecimalDistribution(colStats);

        Assert.assertEquals(2, dist.selectivity(new BigDecimal(101))); // return min of 2, not actual 1
        Assert.assertEquals(2, dist.selectivity(new BigDecimal(102)));
        Assert.assertEquals(3, dist.selectivity(new BigDecimal(103)));
        Assert.assertEquals(4, dist.selectivity(new BigDecimal(104)));
        Assert.assertEquals(0, dist.selectivity(new BigDecimal(105)));
    }
	
    @Test
    public void testDistributionWorksWithSingleElement() throws Exception{
        //the test is to make sure that we can create the entity without it breaking
        FrequencyCounter<? super BigDecimal> counter=FrequencyCounters.counter(ComparableComparator.<BigDecimal>newComparator(),4);
        @SuppressWarnings("unchecked")
		ColumnStatistics<BigDecimal> scs = new ComparableColumnStatistics<>(0,
                CardinalityEstimators.hyperLogLogBigDecimal(4),
                (FrequentElements<BigDecimal>)counter.frequentElements(4),
                BigDecimal.ONE,
                BigDecimal.ONE,
                2,
                12,
                0,
                3,
                new DistributionFactory<BigDecimal>(){
                    @Override
                    public Distribution<BigDecimal> newDistribution(ColumnStatistics<BigDecimal> statistics){
                        return new UniformDecimalDistribution(statistics);
                    }
                });

        UniformDecimalDistribution dist=new UniformDecimalDistribution(scs);
        /*
         * We need to make sure of the following things:
         *
         * 1. values == minValue or max return the correct count
         * 2. Values != minValue return 0
         * 3. Range estimates which include the minValue return minValueCount
         * 4. Range estimates which do not include the minValue return 0
         */
        Assert.assertEquals(scs.minCount(),dist.selectivity(scs.minValue()));
        Assert.assertEquals(0l,dist.selectivity((scs.minValue().add(BigDecimal.ONE))));

        Assert.assertEquals(scs.minCount(),dist.rangeSelectivity(scs.minValue(),(scs.minValue().add(BigDecimal.ONE)),true,true));
        Assert.assertEquals(0,dist.rangeSelectivity(scs.minValue(),(scs.minValue().add(BigDecimal.ONE)),false,true));
    }

    @Test
    public void emptyDistributionReturnsZeroForAllEstimates() throws Exception{
        //the test is to make sure that we can create the entity without it breaking
        FrequencyCounter<? super BigDecimal> counter=FrequencyCounters.counter(ComparableComparator.<BigDecimal>newComparator(),4);
        @SuppressWarnings("unchecked")
		ColumnStatistics<BigDecimal> scs = new ComparableColumnStatistics<>(0,
                CardinalityEstimators.hyperLogLogBigDecimal(4),
                (FrequentElements<BigDecimal>)counter.frequentElements(4),
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                0,
                0,
                0,
                0,
                new DistributionFactory<BigDecimal>(){
                    @Override
                    public Distribution<BigDecimal> newDistribution(ColumnStatistics<BigDecimal> statistics){
                        return new UniformDecimalDistribution(statistics);
                    }
                });

        UniformDecimalDistribution dist=new UniformDecimalDistribution(scs);
        /*
         * We need to make sure we return 0 in the following scenarios:
         *
         * 1. values == scs.minValue()
         * 2. Values != minValue return 0
         * 3. Range estimates which include scs.minValue()
         * 4. Range estimates which do not include the minValue return 0
         */
        Assert.assertEquals(0,dist.selectivity(scs.minValue()));
        Assert.assertEquals(0l,dist.selectivity((scs.minValue().add(BigDecimal.ONE))));

        Assert.assertEquals(0,dist.rangeSelectivity(scs.minValue(),(scs.minValue().add(BigDecimal.ONE)),true,true));
        Assert.assertEquals(0,dist.rangeSelectivity(scs.minValue(),(scs.minValue().add(BigDecimal.ONE)),false,true));
    }
}