package com.splicemachine.stats.random;

import com.splicemachine.stats.estimate.LongDistribution;

/**
 * Generates a sequence of random numbers according to an arbitrary distribution
 * @author Scott Fines
 *         Date: 7/2/15
 */
public class LongEmpiricalRejectionGenerator implements RandomGenerator{
    private final LongDistribution distribution;
    private final RandomGenerator  uniformGenerator;

    public LongEmpiricalRejectionGenerator(LongDistribution distribution,RandomGenerator rng){
        this.distribution=distribution;
        this.uniformGenerator=rng;
    }

    @Override
    public double nextDouble(){
        return nextLong()/((double)distribution.totalCount());
    }

    @Override
    public int nextInt(){
        long n = nextLong();
        return (int)n;
    }

    @Override
    public boolean nextBoolean(){
        //TODO -sf- cache this?
        double midPoint = (distribution.max()+distribution.min())/2d;
        return nextLong()>midPoint;
    }

    @Override
    public long nextLong(){
        while(true){
            double x=uniformGenerator.nextDouble(); //uniform in [0,1)
            x=x*(distribution.max()-distribution.min())+distribution.min(); //uniform in [min,max)
            long xl = Math.round(x);
            double y=uniformGenerator.nextDouble();
            double prob=distribution.selectivity(xl)/((double)distribution.totalCount());
            if(y<prob) return xl;
        }
    }
}
