package com.splicemachine.stats.cardinality;

import com.splicemachine.hash.HashFunctions;
import com.splicemachine.testutil.ParallelTheoryRunner;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.Random;

/**
 * @author Scott Fines
 * Date: 3/27/14
 */
@RunWith(ParallelTheoryRunner.class)
@Ignore("Not sure that this actually works")
public class AdjustedHyperLogLogCounterTest {

    @DataPoint public static CardinalityTestData lowCardinality     = new CardinalityTestData(30,100        ,1000000);
    @DataPoint public static CardinalityTestData lowMidCardinality  = new CardinalityTestData(30,10000      ,1000000);
    @DataPoint public static CardinalityTestData midCardinality     = new CardinalityTestData(30,100000     ,1000000);
    @DataPoint public static CardinalityTestData midHighCardinality = new CardinalityTestData(30,1000000    ,1000000);
    @DataPoint public static CardinalityTestData highCardinality    = new CardinalityTestData(30,10000000   ,10000000);
    @DataPoints public static int[] precisions = new int[]{4,5,6,7,8,9,10,11,12,13,14,15,16};

    @Theory
    public void errorIsNormallyDistributed(int precision,CardinalityTestData testData) throws Exception {
        testErrorDistribution(testData.numTrials,precision,testData.cardinality,testData.size);
    }

    protected void testErrorDistribution(int numTrials, int precision, int numDistincts, int numElements) {
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
            IntCardinalityEstimator estimator = CardinalityEstimators.hyperLogLogInt(precision,HashFunctions.murmur2_64(0));
            double rawError=CardinalityTest.test(estimator,numElements,numDistincts,random);
            error[i] =rawError;
            hasPositiveError = hasPositiveError || rawError!=0d;
        }

        if(!hasPositiveError) return; //that's as accurate as we can get

        KolmogorovSmirnovTest test=new KolmogorovSmirnovTest();
        Assert.assertTrue("Error is not normally distributed!",test.kolmogorovSmirnovTest(dist,error,0.01));
    }

    private static class CardinalityTestData{
        final int numTrials;
        final int cardinality;
        final int size;

        public CardinalityTestData(int numTrials,int cardinality,int size){
            this.numTrials=numTrials;
            this.cardinality=cardinality;
            this.size=size;
        }

        @Override
        public String toString(){
            return "("+numTrials+","+cardinality+","+size+")";
        }
    }

}
