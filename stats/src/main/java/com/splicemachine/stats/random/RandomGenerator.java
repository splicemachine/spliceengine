package com.splicemachine.stats.random;

/**
 * Represents a distribution of randomly-generated numbers in the range [0,1).
 *
 * @author Scott Fines
 *         Date: 12/2/14
 */
public interface RandomGenerator{

    double nextDouble();

    int nextInt();

    boolean nextBoolean();

    long nextLong();
}
