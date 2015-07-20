package com.splicemachine.stats.cardinality;

import com.google.common.collect.Lists;
import com.splicemachine.hash.HashFunctions;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Random;

/**
 * @author Scott Fines
 * Date: 3/27/14
 */
@RunWith(Parameterized.class)
//@Ignore
public class AdjustedHyperLogLogCounterTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> data = Lists.newArrayList();
        for(int i=4;i<=16;i++){
            data.add(new Object[]{i});
        }
        return data;
    }

    private final int precision;

    public AdjustedHyperLogLogCounterTest(int precision) {
        this.precision = precision;
    }

    @Test
//		@Ignore
    public void testErrorIsNormallyDistributedHighCardinality() throws Exception {
		/*
		 * HyperLogLog is accurate within a Normal Distribution. That is,
		 * with an error threshold of s = 1.04/sqrt(2^precision),
		 * 68% of the time, the relative error will be <= s,
		 * 95% of the time, it will be <= 2*s, and 99.7% of the time, it will
		 * be <= 3*s.
		 *
		 * To test this, we run the same computation a couple hundred times, recording
		 * the relative error each time, then make sure that the distributions match at the end.
		 *
		 */

        int numTrials = 1000;
        int numDistincts = 10000;
        int numElements = 50000;
        testErrorDistribution(numTrials, precision, numDistincts, numElements);
    }

    @Test
    public void testErrorIsNormallyDistributedLowCardinality() throws Exception {
				/*
				 * HyperLogLog is accurate within a Normal Distribution. That is,
				 * with an error threshold of s = 1.04/sqrt(2^precision),
				 * 68% of the time, the relative error will be <= s,
				 * 95% of the time, it will be <= 2*s, and 99.7% of the time, it will
				 * be <= 3*s.
				 *
				 * To test this, we run the same computation a couple hundred times, recording
				 * the relative error each time, then make sure that the distributions match at the end.
				 */

        int numTrials = 100;
        int numDistincts = 10;
        int numElements = 100;
        testErrorDistribution(numTrials, precision, numDistincts, numElements);
    }

    protected void testErrorDistribution(int numTrials, int precision, int numDistincts, int numElements) {
        double sigma = 1.04d/Math.sqrt(1<<precision);
        NormalDistribution dist = new NormalDistribution(0,sigma);

        Random random = new Random(0);
        double[] error = new double[numTrials];
        /*
         * It's always possible that the cardinality estimates are spot on in all cases--
         * in this scenario, the error is not normally distributed, it's uniformly distributed
         * at 0. To check this, we just keep track of whether or not we see a positive error.
         */
        boolean hasPositiveError = false;
        for(int i=0;i<numTrials;i++){
            BaseLogLogCounter estimator = new AdjustedHyperLogLogCounter(precision, HashFunctions.murmur2_64(0));
            double rawError=CardinalityTest.test(estimator,numElements,numDistincts,random);
            error[i] =rawError;
            hasPositiveError = hasPositiveError || rawError!=0d;
        }

        if(!hasPositiveError) return; //that's as accurate as we can get

        KolmogorovSmirnovTest test=new KolmogorovSmirnovTest();
        Assert.assertTrue("Error is not normally distributed!",test.kolmogorovSmirnovTest(dist,error,0.01));
    }
}
