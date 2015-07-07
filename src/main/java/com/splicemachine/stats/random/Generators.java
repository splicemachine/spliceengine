package com.splicemachine.stats.random;

import com.splicemachine.stats.estimate.LongDistribution;

import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 12/2/14
 */
public class Generators{

    private Generators(){}

    public static RandomGenerator gaussian(RandomGenerator uniformDistribution, double stdDev){
        return new ShiftedGaussian(new GaussianGenerator(uniformDistribution),0,stdDev);
    }

    public static RandomGenerator gaussian(RandomGenerator uniformDistribution, double mean, double stdDev){
        return new ShiftedGaussian(new GaussianGenerator(uniformDistribution),mean,stdDev);
    }

    public static RandomGenerator fromDistribution(LongDistribution distribution, Random random){
        return new LongEmpiricalRejectionGenerator(distribution,new UniformGenerator(random));
    }

    public static RandomGenerator fromDistribution(LongDistribution distribution, RandomGenerator rng){
        return new LongEmpiricalRejectionGenerator(distribution,rng);
    }

    public static RandomGenerator fromDistribution(LongDistribution distribution){
        return new LongEmpiricalRejectionGenerator(distribution,new UniformGenerator(new Random()));
    }

    public static RandomGenerator uniform(Random random){
        return new UniformGenerator(random);
    }

    public static RandomGenerator uniform(){
        return new UniformGenerator(new Random());
    }

    private static class ShiftedGaussian implements RandomGenerator{
        private final GaussianGenerator baseGaussian;
        private final double mean;
        private final double stdDev;

        public ShiftedGaussian(GaussianGenerator baseGaussian, double mean, double stdDev) {
            this.baseGaussian = baseGaussian;
            this.mean = mean;
            this.stdDev = stdDev;
        }

        @Override public double nextDouble() { return baseGaussian.nextDouble()*stdDev+mean; }
        @Override public int nextInt() { return (int)Math.floor(nextDouble()); }
        @Override public boolean nextBoolean() { return baseGaussian.nextBoolean(); }

        @Override
        public long nextLong(){
            return (long)Math.floor(nextDouble());
        }
    }
}
