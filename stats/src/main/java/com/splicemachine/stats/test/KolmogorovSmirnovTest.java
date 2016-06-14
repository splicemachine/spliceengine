package com.splicemachine.stats.test;

import com.splicemachine.stats.estimate.LongDistribution;
import com.splicemachine.stats.random.Generators;
import com.splicemachine.stats.random.RandomGenerator;

import java.util.Arrays;

/**
 * @author Scott Fines
 *         Date: 6/30/15
 */
public class KolmogorovSmirnovTest{

    private final int sampleSize;
    private final RandomGenerator rng;

    public KolmogorovSmirnovTest(int sampleSize){
       this(sampleSize,Generators.uniform());
    }

    public KolmogorovSmirnovTest(int sampleSize,RandomGenerator uniformRng){
        this.sampleSize=sampleSize;
        this.rng = uniformRng;
    }

    public double statistic(LongDistribution test, LongDistribution reference){
        /*
         * It is possible that the min and max values of the reference distribution don't match that
         * of the test distribution (although we would hope that users will be smart enough to ensure
         * that they are). When that happens, we choose the intersection of the two. If the intersection
         * is empty, then we throw an error
         */
        long max=getMax(test,reference);
        long min=getMin(test,reference);

        if(min>max)
            throw new IllegalArgumentException("The Reference and test distributions do not intersect!");

//        int sampleS = sampleSize;
//        long totalDistance = max-min+1;
//        if(totalDistance<sampleSize){
//            /*
//             * When we have fewer available points in our distribution than we have sample points, then
//             * just sample every point
//             */
//            sampleS = (int)totalDistance;
//        }

        return computeStat(test,reference,sampleSize);
    }

    public boolean test(LongDistribution test, LongDistribution reference, double tolerance){
        /*
         * It is possible that the min and max values of the reference distribution don't match that
         * of the test distribution (although we would hope that users will be smart enough to ensure
         * that they are). When that happens, we choose the intersection of the two. If the intersection
         * is empty, then we throw an error
         */
        long max=getMax(test,reference);
        long min=getMin(test,reference);

        if(min>max)
            throw new IllegalArgumentException("The Reference and test distributions do not intersect!");

        int sampleS = sampleSize;
        long totalDistance = max-min+1;
        if(totalDistance<sampleSize){
            /*
             * When we have fewer available points in our distribution than we have sample points, then
             * just sample every point
             */
            sampleS = (int)totalDistance;
        }
        double maxDiff=computeStat(test,reference,sampleS);

        double kStat = computeKStat(sampleS,sampleS,tolerance);
        return maxDiff<=kStat;
    }

    /* *****************************************************************************************************************/
    /*private helper methods*/

    private long getMin(LongDistribution test,LongDistribution reference){
        long min = test.min();
        long rMin = reference.min();
        if(rMin>min)
            min = rMin;
        return min;
    }


    private long getMax(LongDistribution test,LongDistribution reference){
        long max = test.max();
        long rMax = reference.max();
        if(rMax<max)
            max = rMax;
        return max;
    }

    private double computeStat(LongDistribution test,
                               LongDistribution reference,
                               int sampleS){
        double maxDiff=0;
        //TODO -sf- bound the test distribution
        RandomGenerator numGen =Generators.fromDistribution(test,this.rng);
        long[] data = new long[sampleS];
        for(int i=0;i<sampleS;i++){
            data[i] = numGen.nextLong();
        }
        Arrays.sort(data);
        double refTotal = reference.totalCount();
        for(int i=1;i<=sampleS;i++){
            double yi = reference.selectivityBefore(data[i-1],true)/refTotal;
            double diff=Math.max(yi-(i-1)/sampleS,i/sampleS-yi);
            if(diff>maxDiff)
                maxDiff = diff;
        }
        return maxDiff;
    }

    private double computeKStat(long n,long en,double alpha){
        double c = c(alpha);
        double scale = n+en/((double)n*en);
        return c*Math.sqrt(scale);
    }

    private static final double[] alphaThresholds = new double[]{0.10,0.05,0.025,0.01,0.005,0.001};
    private static final double[] c = new double[]{1.22,1.36,1.48,1.63,1.73,1.95};
    private double c(double alpha){
        for(int i=0;i<alphaThresholds.length;i++){
            if(alpha>=alphaThresholds[i]){
                return c[i];
            }
        }
        return alphaThresholds[alphaThresholds.length];
    }
}
