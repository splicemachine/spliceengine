package com.splicemachine.stats.random;

import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 12/2/14
 */
public class Distributions {

    private Distributions(){}

    public static RandomDistribution gaussian(RandomDistribution uniformDistribution, double mean, double stdDev){
        return new ShiftedGaussian(new GaussianDistribution(uniformDistribution),mean,stdDev);
    }

    public static RandomDistribution uniform(Random random){
        return new UniformDistribution(random);
    }

    public static RandomDistribution uniform(){
        return new UniformDistribution(new Random());
    }


    private static class ShiftedGaussian implements RandomDistribution{
        private final GaussianDistribution baseGaussian;
        private final double mean;
        private final double stdDev;

        public ShiftedGaussian(GaussianDistribution baseGaussian, double mean, double stdDev) {
            this.baseGaussian = baseGaussian;
            this.mean = mean;
            this.stdDev = stdDev;
        }

        @Override public double nextDouble() { return baseGaussian.nextDouble()*stdDev+mean; }
        @Override public int nextInt() { return (int)Math.floor(nextDouble()); }
        @Override public boolean nextBoolean() { return baseGaussian.nextBoolean(); }
    }
}
