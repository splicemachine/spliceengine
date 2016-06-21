package com.splicemachine.stats.random;

import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 12/2/14
 */
public class UniformGenerator implements RandomGenerator{
    private final Random random;

    public UniformGenerator(Random random) {
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
    public long nextLong() {
        return random.nextLong();
    }

    @Override
    public boolean nextBoolean() {
        return random.nextBoolean();
    }
}
