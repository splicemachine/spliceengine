package com.splicemachine.stats.histogram;


import com.carrotsearch.hppc.IntDoubleOpenHashMap;
import com.splicemachine.stats.order.IntMinMaxCollector;

/**
 * @author Scott Fines
 *         Date: 11/26/14
 */
class IntRangeSolver implements IntRangeQuerySolver{
    private final Level[] levels;
    private final double avg;
    private final int posShift;
    private final IntDoubleOpenHashMap coefMap;
    private final int maxObserved;
    private final int minObserved;

    private final long equalsMax;
    private final long equalsMin;

    IntRangeSolver(int N,
                   IntMinMaxCollector maxCollector,
                   IntDoubleOpenHashMap coefMap){
        this.maxObserved = maxCollector.max();
        this.minObserved = maxCollector.min();
        this.levels = buildLevels(N);
        this.avg = coefMap.get(0)/Math.sqrt(N);
        this.posShift=N>>1;
        this.coefMap = coefMap;
        this.equalsMax = maxCollector.maxCount();
        this.equalsMin = maxCollector.minCount();
    }

    @Override public int max() { return maxObserved; }
    @Override public int min() { return minObserved; }

    @Override
    public long equal(int n){
        if(n<minObserved|| n>maxObserved) return 0;
        if(n==minObserved) return equalsMin;
        else if(n==maxObserved) return equalsMax;
        return estimateEquals(n);
    }

    @Override
    public long after(int value, boolean equals) {
        long estimate = estimateBetween(value,maxObserved)+equalsMax;
        if(!equals)
            estimate-=estimateEquals(value);
        return estimate;
    }

    @Override
    public long before(int value, boolean equals) {
        long estimate = 0;
        boolean checkEquals = true;
        if(value>maxObserved){
            estimate+=equalsMax;
            value = maxObserved;
            checkEquals=false;
        }
        estimate += estimateBetween(minObserved,value);
        if(equals&&checkEquals){
            estimate+=estimateEquals(value);
        }
        return estimate;
    }

    @Override
    public long between(int startValue,
                        int endValue,
                        boolean inclusiveStart,
                        boolean inclusiveEnd) {
        long estimate=0;
        if(startValue>maxObserved) return 0l;
        else if(startValue==maxObserved){
            if(inclusiveStart) return equalsMax;
            else return 0l;
        }

        if(endValue<minObserved) return 0l;
        else if(endValue==minObserved) {
            if(inclusiveEnd) return equalsMin;
            else return 0;
        }

        if(startValue<minObserved) {
            startValue = minObserved;
            inclusiveStart=true;
        } else if (startValue==minObserved &&!inclusiveStart){
            inclusiveStart=true;
            estimate-=equalsMin;
        }

        if(endValue>maxObserved){
            endValue=maxObserved;
            inclusiveEnd=true;
        }else if(endValue==maxObserved && inclusiveEnd){
            estimate+=equalsMax;
            inclusiveEnd = false;
        }
        estimate += estimateBetween(startValue,endValue);
        if(!inclusiveStart){
            estimate-=estimateEquals(startValue);
        }if(inclusiveEnd)
            estimate+=estimateEquals(endValue);
        return estimate;
    }

    @Override
    public long getNumElements(Integer start, Integer end, boolean inclusiveStart, boolean inclusiveEnd) {
        int startValue = start!=null? start:minObserved;
        int endValue = end!=null? end: maxObserved;
        return between(startValue,endValue,inclusiveStart,inclusiveEnd);
    }

    @Override
    public Integer getMin() {
        return minObserved;
    }

    @Override
    public Integer getMax() {
        return maxObserved;
    }

    /*******************************************************************/
    /*private helper methods*/

    private long estimateBetween(int a, int b){
        assert a<b : "b<a!";

        double betweenEst = (b-a)*avg+levels[0].estimateBetween(a,b);
        return (long)betweenEst;
    }

    private Level[] buildLevels(int N) {
        int lg =0;
        int s = 1;
        while(s<N){
            s<<=1;
            lg++;
        }
        Level[] levels= new Level[lg];
        for(int i=0;i<levels.length;i++){
            levels[i] = new Level(lg,i);
        }
        return levels;
    }

    private long estimateEquals(int n){
        double curr = avg;
        for(int i=0;i<levels.length;i++){
            double v = levels[i].levelValue(n);
            curr+= v;
        }
        return (long)curr;
    }

    private class Level{
        /*the length of the dyadic intervals at this level*/
        private final int groupSize;
        /*The shift to apply when computing the group*/
        private final double positionalShift;
        /*Constant to scale coefficients by*/
        private final double scale;
        /*Shift for finding the proper coefficient*/
        private final int levelPow;
        /*The height of this level in the tree(counting from 0)*/
        private final int level;

        public Level(int lg, int level) {
            this.levelPow = 1<<level;
            this.level = level;

            groupSize = 1<<(lg-level);
            if(level==0) positionalShift =1d/2;
            else positionalShift = levelPow>>1;
            scale = Math.sqrt(groupSize);
        }

        private boolean signum(int n){
           /*
            * represents the k_l(n) function. Determines
            * the sign of the added coefficient. When this
            * returns true, then the coefficient is positive.
            * Otherwise, the coefficient is negative.
            */
            return ((n+posShift)/(groupSize >>1)+1)%2!=0;
        }

        private int group(int n){
            /*
             * represents the g_l(n) function. Finds
             * the group for a specific value
             */
            return (int)(n/(double) groupSize + positionalShift);
        }

        private int startValue(int group){
            /*
             * Finds the smallest value n such that
             * group(n) = group.
             *
             * Because group(n) uses a floor function,
             * we can just reverse the algebra of
             * the group() function here.
             */
            double s = group- positionalShift;
            s*= groupSize;
            return (int)s;
        }

        /*
         * Estimate the frequency of elements
         * which fit within the interval [a,b).
         *
         * This uses recursion, but the maximum stack size
         * is the height of the tree, which is lg(N). Since we
         * are working only with integers here, lg(N) is at most
         * 32, so the max call stack is only 32; hence, StackOverflowErrors
         * are not a concern here.
         */
        private double estimateBetween(int a, int b){
            /*
             * We set it up so that [a,b) will belong in the same
             * dyadic range; therefore, we assume [a,b) will
             * both have the same group. We only need to determine
             * if we straddle the midpoint or not.
             */
            int group = group(a);
            int start = startValue(group);
            int stop = start+ groupSize;
            if(a==start && b ==stop) return 0;

            int midPoint = (start+stop)/2;
            double coef = coefMap.get(group+levelPow)/scale;
            if(a>=midPoint){
                /*
                 * by assumption, b>a, so we know that both a and b
                 * fit entirely to the right of the midpoint. In this case,
                 * we add (b-a)*coef to the sum of the lower dyadic range values
                 */
                return betweenNextLevel(a, b)+(b-a)*coef;
            }else if(b<=midPoint){
                /*
                 * we know that [a,b) fits entirely to the left of the midpoint.
                 * In this case, we subtract (b-a)*coef from the sum of the
                 * lower dyadic range values
                 */
                return betweenNextLevel(a, b)-(b-a)*coef;
            }else{
                /*
                 * we straddle the midpoint. In this case, we subtract
                 * the value of (mid-a), and add (b-mid), plus lower values on
                 * each side
                 */
                double sum = betweenNextLevel(a, midPoint);
                sum-=(midPoint-a)*coef;
                sum+= betweenNextLevel(midPoint, b);
                sum+=(b-midPoint)*coef;
                return sum;
            }
        }

        private double betweenNextLevel(int a, int b) {
            /*
             * Recursive call to estimate the value of [a,b)
             * at the next level down in the tree.
             */
            if(level==levels.length-1) return 0;
            Level next = levels[level+1];
            return next.estimateBetween(a,b);
        }

        private double levelValue(int n){
            double coef = coefMap.get(group(n)+levelPow);
            if(coef==0) return 0; //we are done

            double value = coef/scale;
            return signum(n) ? -value : value;
        }
    }
}
