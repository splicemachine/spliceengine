package com.splicemachine.stats.histogram;


import com.carrotsearch.hppc.IntDoubleOpenHashMap;

/**
 * @author Scott Fines
 *         Date: 11/26/14
 */
public class IntRangeSolver {
    private final Level[] levels;
    private final double avg;
    private final int posShift;
    private final IntDoubleOpenHashMap coefMap;

    IntRangeSolver(int N,IntDoubleOpenHashMap coefMap){
        int lg =0;
        int s = 1;
        while(s<N){
            s<<=1;
            lg++;
        }
        this.levels = new Level[lg];
        for(int i=0;i<levels.length;i++){
            levels[i] = newLevel(lg,i);
        }
        this.avg = coefMap.get(0)/Math.sqrt(N);
        this.posShift=N>>1;
        this.coefMap = coefMap;
    }

    public long estimateEquals(int n){
        double curr = avg;
        for(int i=0;i<levels.length;i++){
            double v = levels[i].levelValue(n);
            curr+= v;
        }
        return (long)curr;
    }

    protected Level newLevel(int lg, int level) {
        return new Level(lg,level);
    }

    private class Level{
        private final int signDrop;
        private final double levelShift;
        private final double scale;
        private final int levelPow;

        public Level(int lg, int level) {
            this.levelPow = 1<<level;

            signDrop = 1<<(lg-level);
            if(level==0) levelShift=1d/2;
            else levelShift = 1<<(level-1);
            scale = Math.sqrt(signDrop);
        }

        private boolean signum(int n){
           /*
            * represents the k_l(n) function
            */
            return ((n+posShift)/(signDrop>>1)+1)%2!=0;
        }

        private int group(int n){
            /*
             * represents the g_l(n) function
             */
            return (int)(n/(double)signDrop+levelShift);
        }

        private double levelValue(int n){
            int group = group(n)+levelPow;
            double coef = coefMap.containsKey(group)? coefMap.get(group): 0d;
            if(coef==0) return 0; //we are done

            double value = coef/scale;
            return signum(n) ? -value : value;
        }
    }
}
