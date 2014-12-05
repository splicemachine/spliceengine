package com.splicemachine.stats.random;

import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 12/2/14
 */
public class UniformDistribution implements RandomDistribution{
    private final Random random;

    public UniformDistribution(Random random) {
        this.random = random;
    }

    @Override
    public double nextDouble() {
        return random.nextDouble();
    }

    @Override
    public int nextInt() {
        return random.nextInt();
    }

    @Override
    public boolean nextBoolean() {
        return random.nextBoolean();
    }
}
