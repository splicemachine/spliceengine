package com.splicemachine.stats.histogram;

import com.carrotsearch.hppc.IntDoubleOpenHashMap;
import com.splicemachine.hash.BooleanHash;
import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.primitives.MoreArrays;
import com.splicemachine.stats.IntUpdateable;
import com.splicemachine.stats.order.IntMinMaxCollector;

import java.util.Arrays;

/**
 * A grouping counter. This is a an equivalent to the "GroupedCountSketch"
 * described in Cormode et al. "Fast Approximate Wavelet Tracking On Streams",
 * and is primarily used as the backing data structure for int wavelet approximations.
 *
 *
 * @author Scott Fines
 *         Date: 10/23/14
 */
class GroupedCounter implements IntUpdateable{
    /*
     * The first index is the level, the second index is the group id
     */
    private final Level[] levels;
    private long sum = 0l;
    private long count = 0l;
    private final int maxValue;
    private final int lg;

    private final IntMinMaxCollector boundaryCollector;

    public static GroupedCounter newCounter(int maxValue,final float tolerance,final int t){
          assert tolerance<1 && tolerance>0: "Tolerance must be between 0 and 1";
           return new GroupedCounter(maxValue){
               @Override
               protected Level newLevel(int level, int lgN) {
                   return new SketchLevel(level,lgN-1,t,tolerance);
               }
           };
    }

    GroupedCounter(int maxValue){
        this.maxValue = maxValue;
        //we allow positive and negative values, so we will have maxValue*2 counters,
        //and lg(maxValue) levels
        int N = 1;
        int lg =0;
        while(N<=maxValue){
            N<<=1;
            lg++;
        }
        this.lg = lg;
        this.levels = new Level[lg];
        for(int i=0;i<levels.length;i++){
            levels[i] = newLevel(i,lg);
        }
        this.boundaryCollector = new IntMinMaxCollector();
    }

    protected Level newLevel(int level, int lgN){
        return new ExactLevel(level,lgN);
    }

    @Override
    public void update(int item) {
        update(item,1l);
    }

    @Override
    public void update(int item, long count) {
        sum+=count;
        //update each interval independently
        //noinspection ForLoopReplaceableByForEach
        for(int i=0;i<levels.length;i++){
            levels[i].update(item,count);
        }
        boundaryCollector.update(item);
        this.count+=count;
    }

    @Override
    public void update(Integer item) {
        update(item,1l);
    }

    @Override
    public void update(Integer item, long count) {
        assert item!=null: "Cannot build a wavelet of a null int!";
        update(item.intValue(),count);
    }

    public long[][] getEnergies(){
        long[][] energies = new long[levels.length][];
        for(int i=0;i<levels.length;i++){
            energies[i] = levels[i].getEnergies();
        }
        return energies;
    }

    public IntDoubleOpenHashMap getCoefficients(double support){
        IntDoubleOpenHashMap coefs = new IntDoubleOpenHashMap();
        double avg = count/Math.sqrt(1<<(lg));
        coefs.put(0,avg);
        findHighestCoefs(support,0,0,levels,coefs);
        return coefs;
    }

    public IntRangeQuerySolver build(double threshold){
        IntDoubleOpenHashMap coefs = getCoefficients(threshold);
        int min = boundaryCollector.min();
        int max = boundaryCollector.max();
        double[] scales = new double[levels.length];
        for(int i=0;i<levels.length;i++){
            scales[i] = levels[i].scale;
        }
        return new IntWaveletQuerySolver(min,max,this.count,coefs,lg,scales);
    }

    private void findHighestCoefs(double threshold,int level, int g, Level[] levels, IntDoubleOpenHashMap coefs) {
        if(level>=levels.length) return;
        double energy = levels[level].getEnergy(g);
        if(energy>=threshold){
            int coef = (1<<level)+g;
            coefs.put(coef,levels[level].getValue(g));
            findHighestCoefs(threshold, level+1, 2*g, levels, coefs);
            findHighestCoefs(threshold, level+1, 2*g+1, levels, coefs);
        }
    }

    private static abstract class Level{
        protected final double a;
        protected final double b;
        protected final int level;
        protected final int lg;
        private final double na;
        private final double nb;

        protected final double scale;

        protected Level(int level, int lg) {
            this.level = level;
            this.lg = lg;
            this.a = 1d/(1<<(lg-level));
            if(level==0)
                this.b = 1d/2;
            else
                this.b = (1<<(level-1));

            this.na = a*2;
            this.nb = b*2;

            this.scale = Math.sqrt(a);
        }

        protected int group(int value) {
            return (int)(a*value+b);
        }

        protected long signedCount(int value,long count) {
            int ng = (int)(na*value+nb);
            return ng%2==0?-count: count;
        }

        public abstract long[] getEnergies();

        public abstract double getEnergy(int group);

        public abstract double getValue(int group);

        public abstract void update(int value, long count);
    }

    private static class ExactLevel extends Level{
        private final long[] counters;

        private ExactLevel(int level, int lg){
            super(level,lg);
            this.counters = new long[1<<level];
        }

        @Override
        public void update(int value, long count){
            int group = group(value);
            long cnt = signedCount(value,count);
            counters[group]+=cnt;
        }

        @Override public long[] getEnergies(){ return counters; }

        @Override
        public double getEnergy(int group) {
            long counter = counters[group];
            return scale*counter*counter;
        }

        @Override
        public double getValue(int group) {
            return scale*counters[group];
        }
    }

    private static class SketchLevel extends Level{

        private final int t;
        private final int b;
        private final int c;

        private final long[][][] s;

        private final Hash32[] h;
        private final Hash32[] f;
        private final BooleanHash[] eps;

        public SketchLevel(int level, int lg,int t, float epsilon){
            super(level,lg);
            this.t = t;

            float size = 1/epsilon;
            int temp = 1;
            while(temp<size)
                temp<<=1;
            this.b = temp;
            size/=epsilon;
            while(temp<size){
                temp<<=1;
            }
            this.c = temp;

            this.s = new long[t][][];
            this.h = new Hash32[t];
            this.f = new Hash32[t];
            this.eps = new BooleanHash[t];
            for(int i=0;i<t;i++){
                s[i] = new long[b][];
                for(int j=0;j<b;j++){
                    s[i][j] = new long[c];
                }

                h[i] = HashFunctions.murmur3(1<<(i-1));
                f[i] = HashFunctions.murmur3(3*i+2);
                eps[i] = HashFunctions.booleanHash(1<<i);
            }
        }

        public void update(int value, long count){
            int group = group(value);
            for(int m=0;m<t;m++){
                int hPos = h[m].hash(group) & (b-1);
                int fPos = f[m].hash(group) & (c-1);

                if(eps[m].hash(group))
                    s[m][hPos][fPos]-=count;
                else
                    s[m][hPos][fPos]+=count;
            }
        }

        @Override
        public long[] getEnergies() {
            long[] counters = new long[1<<level];
            for(int i=0;i<counters.length;i++){
                counters[i] = estimateEnergy(i);
            }
            return counters;
        }

        @Override
        public double getValue(int group) {
            throw new UnsupportedOperationException("IMPLEMENT");
        }

        @Override
        public double getEnergy(int group) {
            return scale*estimateEnergy(group);
        }

        public long estimateEnergy(int group) {
            long[] possibleValues = new long[t];
            for(int m=0;m<t;m++){
                long energy = 0l;
                int hPos = h[m].hash(group) & (b-1);
                for(int j=0;j<c;j++){
                    long l = s[m][hPos][j];
                    energy+= l*l;
                }
                possibleValues[m] = energy;
            }
//            return MoreArrays.median(possibleValues);
//            return (long)(Math.sqrt(MoreArrays.min(possibleValues)));
            return (long)(Math.sqrt(MoreArrays.median(possibleValues)));
        }
    }


    public static void main(String... args) throws Exception{
//        int numIterations =1000;
//        int max=8;
//        Random random = new Random();
//        GroupedCounter counter = GroupedCounter.newCounter(max,0.1f,3);
//        GroupedCounter correctCounter = new GroupedCounter(max);
//        long correctEnergy = 0l;
//        for(int i=0;i<numIterations;i++){
//            int next =random.nextInt(2*max)-max;
//            counter.update(next,1l);
//            correctCounter.update(next,1l);
//            correctEnergy+=next*next;
//        }
//        long[][] energies = counter.getEnergies();
//        System.out.printf("Actual : %s%n",Arrays.deepToString(energies));
//        System.out.printf("Correct: %s%n", Arrays.deepToString(correctCounter.getEnergies()));
//
//        System.out.printf("Correct energy: %d%n",correctEnergy);
//        long[] lowestLevel = energies[energies.length-1];
//        long estEnergy = 0l;
//        int level = energies.length-1;
//        for(int i=0;i<lowestLevel.length;i++){
//            long gCount = lowestLevel[i]; //the count of the "group"--must remap to original component to get actual energy
//            int v = i-(1<<(level-1));
//
//            estEnergy+=(v*v)*gCount;
//        }
//        System.out.printf("Est. energy: %d%n",estEnergy);

//        int[] signal = new int[]{0,0,2,2,2,2,2,3,3};
        int[] signal = new int[]{0,1,2,3,4,5,6,7};
//        int[] count = new int[]{1,3,5,11,12,13,0,1};
        int[] count = new int[]{2,2,0,2,3,5,4,4};
        int N =8;
        GroupedCounter exact = new GroupedCounter(N);
        for(int i=0;i<signal.length;i++){
            exact.update(signal[i],count[i]);
        }

        System.out.println(Arrays.deepToString(exact.getEnergies()));
        System.out.println(exact.getCoefficients(0.0d));
    }
}
