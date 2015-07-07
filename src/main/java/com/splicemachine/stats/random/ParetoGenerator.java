package com.splicemachine.stats.random;

/**
 * @author Scott Fines
 *         Date: 12/2/14
 */
public class ParetoGenerator implements RandomGenerator{
    private final RandomGenerator uniform;
    private final double b;
    private final double alpha;

    public ParetoGenerator(RandomGenerator uniform,double b,double alpha) {
        this.uniform = uniform;
        this.b = b;
        this.alpha = 1/alpha;
    }

    @Override
    public double nextDouble() {
        double r = uniform.nextDouble();
        return b/Math.pow(1-r,alpha);
    }

    @Override
    public int nextInt() {
        return (int)nextDouble();
    }

    @Override
    public long nextLong() {
        return (long)nextDouble();
    }

    @Override
    public boolean nextBoolean() {
        return uniform.nextBoolean();
    }
}
