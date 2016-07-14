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

import com.splicemachine.stats.collector.ColumnStatsCollectors;
import com.splicemachine.stats.collector.LongColumnStatsCollector;
import com.splicemachine.stats.random.Generators;
import com.splicemachine.stats.random.RandomGenerator;
import com.splicemachine.stats.random.UniformGenerator;
import com.splicemachine.stats.test.KolmogorovSmirnovTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theory;

import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 6/30/15
 */
//@RunWith(Theories.class)
@Ignore("Not ready yet")
public class LongUniformDistributionTest{

//    @DataPoint public static final TestData allPositive = new TestData(0l,0l,100l,10000l,10,0.001d);
    @DataPoint public static final TestData positiveNegative= new TestData(0l,-100l,100l,10000l,10,0.001d);

    @Theory
    public void matchesAUniformDistribution(TestData test) throws Exception{
        LongColumnStatsCollector collector=ColumnStatsCollectors.longCollector(0,8,5);
        Random random = new Random(test.seed);
        long max=test.max;
        long min=test.min;
        long[] counts = new long[(int)(max-min+1)];
        for(int i=0;i<test.numIterations;i++){
            long next =Math.round(random.nextDouble()*(max-min)+min);
            counts[(int)(next-min)]++;
            collector.update(next);
        }
        LongDistribution reference = new SpecifiedLongDistribution(counts,min,max);
        LongDistribution uniform = new UniformLongDistribution(collector.build());

        Assert.assertTrue("Distribution does not match!",new KolmogorovSmirnovTest(test.testSampleSize).test(uniform,reference,test.tolerance));
    }

    @Theory
    public void doesNotMatchAGaussianDistribution(TestData test) throws Exception{
        LongColumnStatsCollector collector=ColumnStatsCollectors.longCollector(0,8,5);
        long max=test.max;
        long min=test.min;
        long midPoint = (max+min)/2;
        RandomGenerator rng =Generators.gaussian(new UniformGenerator(new Random(test.seed)),midPoint,(max-min)/8d);
        long[] counts = new long[(int)(max-min+1)];
        for(int i=0;i<test.numIterations;i++){
            long next =Math.round(rng.nextDouble());
            if(next<min||next>max) {
                i--;
                continue;
            }
            counts[(int)(next-min)]++;
            collector.update(next);
        }
        LongDistribution reference = new SpecifiedLongDistribution(counts,min,max);
        LongDistribution uniform = new UniformLongDistribution(collector.build());

        Assert.assertFalse("Distribution matches erroneously!",new KolmogorovSmirnovTest(test.testSampleSize).test(uniform,reference,test.tolerance));
    }

    private static class TestData{
        final long seed;
        final long min;
        final long max;
        final long numIterations;
        final int testSampleSize;
        final double tolerance;

        public TestData(long seed,long min,long max,long numIterations,int testSampleSize,double tolerance){
            this.seed=seed;
            this.min=min;
            this.max=max;
            this.numIterations=numIterations;
            this.testSampleSize=testSampleSize;
            this.tolerance=tolerance;
        }

        @Override
        public String toString(){
            return "TestData{"+
                    seed+
                    ','+min+
                    ','+max+
                    ','+numIterations+
                    ','+testSampleSize+
                    ','+tolerance+
                    '}';
        }
    }

}
