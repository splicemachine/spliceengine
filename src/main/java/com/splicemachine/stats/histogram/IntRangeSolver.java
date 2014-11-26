package com.splicemachine.stats.histogram;

/**
 * @author Scott Fines
 *         Date: 11/26/14
 */
public class IntRangeSolver {

    private static class DyadicRange{
        private final int level;
        private final int group;
        private final double coefficient;

        public DyadicRange(int level, int group, double coefficient) {
            this.level = level;
            this.group = group;
            this.coefficient = coefficient;
        }

    }
}
