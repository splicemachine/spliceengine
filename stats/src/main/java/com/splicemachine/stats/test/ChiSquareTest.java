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

import com.splicemachine.stats.estimate.LongDistribution;
import com.splicemachine.stats.random.RandomGenerator;

/**
 * Performs the Chi-Square computation necessary for the Chi-Square statistical test.
 *
 * The Chi-Square test is a test to determine whether a given random distribution matches a known distribution. For more
 * information, see <em>The Art of Computer Programming, Volume 2: Seminumerical Algorithms</em> by Donald Knuth,
 * or refer to <a href="https://en.wikipedia.org/wiki/Chi-squared_test">Wikipedia</a>.
 *
 * This particular implementation is of the <em>Pearson's Chi-Squared test</em>, which only works for discrete values.
 * Thus, we are asked to
 * @author Scott Fines
 *         Date: 6/29/15
 */
public class ChiSquareTest{

    private final RandomGenerator rng;
    private final int numTests;

    public ChiSquareTest(RandomGenerator rng, int numTests){
        this.rng=rng;
        this.numTests = numTests;
    }

    /**
     * Tests that the distribution provided by the {@code test} distribution matches (within
     * a given {@code tolerance}) a given reference distribution.
     *
     * @param test the distribution to test
     * @param reference the reference distribution
     * @param tolerance If the chi-square statistic exceeds this parameter, then the test fails
     * @return true if the test distribution is within a tolerance of the reference distribution, false
     * otherwise
     * @throws IllegalArgumentException when the intersection of {@code test} and {@code reference} is the empty set
     */
    public boolean test(LongDistribution test,LongDistribution reference, double tolerance){
        /*
         * It is possible that the min and max values of the reference distribution don't match that
         * of the test distribution (although we would hope that users will be smart enough to ensure
         * that they are). When that happens, we choose the intersection of the two. If the intersection
         * is empty, then we throw an error
         */
        long min = test.min();
        long max = test.max();
        long rMax = reference.max();
        if(rMax<min)
            throw new IllegalArgumentException("The Reference and test distributions do not intersect!");
        else if(rMax<max)
            max = rMax;
        long rMin = reference.min();
        if(rMin>max)
            throw new IllegalArgumentException("The Reference and test distributions do not intersect!");
        else if(rMin>min)
            min = rMin;

        if(min>max)
            throw new IllegalArgumentException("The Reference and test distributions do not intersect!");

        double chiSquare = 0d;
        for(int i=0;i<numTests;i++){
            long next = rng.nextLong();
            long expected = reference.selectivity(next);
            long actual = test.selectivity(next);
            double sqDiff = Math.pow(expected-actual,2);
            chiSquare+=sqDiff/actual;
        }

        return false;
    }
}
