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

package com.splicemachine.stats.test;

import com.splicemachine.stats.estimate.ActualUniformLongDistribution;
import com.splicemachine.stats.estimate.LongDistribution;
import com.splicemachine.stats.random.Generators;
import com.splicemachine.stats.random.LongEmpiricalRejectionGenerator;
import com.splicemachine.stats.random.RandomGenerator;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.exception.NumberIsTooLargeException;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theory;

import java.util.Random;

/**
 * Tests to make sure that the Kolmogorov-Smirnov test is correctly implemented.
 *
 * @author Scott Fines
 *         Date: 6/30/15
 */
//@RunWith(Theories.class)
    @Ignore
public class KolmogorovSmirnovTestTest{

    @DataPoint public static LongDistribution uniform = new ActualUniformLongDistribution(0,10,2);
    @DataPoint public static LongDistribution uniform2 = new ActualUniformLongDistribution(-10,10,2);

    @Theory
    public void passesWhenTestingItself(LongDistribution dist) throws Exception{
        KolmogorovSmirnovTest tester=new KolmogorovSmirnovTest(10,Generators.uniform(new Random(0)));
        Assert.assertTrue("Does not pass when referring to itself!",tester.test(dist,dist,0d));
    }

    @Theory
    public void failsWhenTestingEmptyDistribution(LongDistribution dist) throws Exception{
        LongDistribution empty = new ActualUniformLongDistribution(0,10,0);
        Assume.assumeTrue("Don't consider the empty distribution",dist.totalCount()>0l);

        KolmogorovSmirnovTest tester=new KolmogorovSmirnovTest(10,Generators.uniform(new Random(0)));
        Assert.assertFalse("Passes against an empty distribution!",tester.test(dist,empty,0d));
    }

    @Theory
    public void matchesCommonsMath(LongDistribution test,final LongDistribution reference){

        int sampleSize=10000;
        RandomGenerator urg=Generators.uniform(new Random(0));
        KolmogorovSmirnovTest tester=new KolmogorovSmirnovTest(sampleSize,urg);
        double ourStat = tester.statistic(test,reference);

        //now do a commons-math version
        org.apache.commons.math3.random.RandomGenerator rg = new JDKRandomGenerator();
        rg.setSeed(0);
        org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest cTest = new org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest(rg);

        RealDistribution rd = new RealDistribution(){
            @Override
            public double probability(double v){
                return reference.selectivity((long)v)/((double)reference.totalCount());
            }

            @Override
            public double density(double v){
                throw new UnsupportedOperationException();
            }

            @Override
            public double cumulativeProbability(double v){
                return reference.selectivityBefore(Math.round(v),true)/((double)reference.totalCount());
            }

            @Override
            public double cumulativeProbability(double v,double v1) throws NumberIsTooLargeException{
                return cumulativeProbability(v1)-cumulativeProbability(v);
            }

            @Override
            public double inverseCumulativeProbability(double v) throws OutOfRangeException{
                return 1-cumulativeProbability(v);
            }

            @Override public double getNumericalMean(){ throw new UnsupportedOperationException(); }
            @Override public double getNumericalVariance(){ throw new UnsupportedOperationException(); }
            @Override public double getSupportLowerBound(){ return reference.min(); }
            @Override public double getSupportUpperBound(){ return reference.max(); }
            @Override public boolean isSupportLowerBoundInclusive(){ return true; }
            @Override public boolean isSupportUpperBoundInclusive(){ return true; }
            @Override public boolean isSupportConnected(){ return true; }
            @Override public void reseedRandomGenerator(long l){ throw new UnsupportedOperationException(); }
            @Override public double sample(){ throw new UnsupportedOperationException(); }
            @Override public double[] sample(int i){ throw new UnsupportedOperationException(); }
        };
        double[] data = new double[sampleSize];
        RandomGenerator rng = new LongEmpiricalRejectionGenerator(test,urg);
        for(int i=0;i<sampleSize;i++){
            data[i] = rng.nextLong();
        }
        double commonStat=cTest.kolmogorovSmirnovStatistic(rd,data);

        Assert.assertEquals("Does not match Commons-math version!",commonStat,ourStat,0.0001d);
    }

    public static void main(String...args) throws Exception{
        double[] sample = new double[]{1,1,1,2,2,4,4,4,6};
        RealDistribution rd = new UniformRealDistribution(0,6);
        org.apache.commons.math3.random.RandomGenerator rg = new JDKRandomGenerator();
        rg.setSeed(0);
        org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest cTest = new org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest(rg);
        System.out.println(cTest.kolmogorovSmirnovStatistic(rd,sample));
    }

}
