package com.splicemachine.testutils;

import java.util.Random;

/**
 * Generates a psuedo-random, Gaussian-distributed sequence of numbers.
 *
 * @author Scott Fines
 *         Date: 10/23/14
 */
public class GaussianRandom {
    private static final double ANGLE_SCALE=2*Math.PI;
    private final Random baseRandom;

    private double next;
    private boolean hasNext = false;
    public GaussianRandom() {
        this(new Random());
    }

    public GaussianRandom(Random baseRandom) {
        this.baseRandom = baseRandom;
    }

    /**
     * @return a gaussian-distributed psuedo-random number, with an average of 0 and a standard
     * deviation of 1.
     */
    public double nextDouble(){
        if(hasNext){
            hasNext=false;
            return next;
        }
        return generatePair();
    }

    private double generatePair() {
        //generate two random numbers in the range of (0,1]
        double u = baseRandom.nextDouble();
        double v = baseRandom.nextDouble();

        double zqrt = Math.sqrt(-2*Math.log(u));
        double trigArg = ANGLE_SCALE*v;
        double z0 = zqrt*Math.cos(trigArg);
        next = zqrt*Math.sin(trigArg);
        hasNext = true;
        return z0;
    }

    public static void main(String...args)throws Exception{
        int size = 10000000;
        GaussianRandom random = new GaussianRandom(new Random());
        double runninVariance=0;
        double runningAvg = 0;
        for(int i=0;i<size;i++){
            double v = random.nextDouble();
            double oldAvg = runningAvg;
            runningAvg = runningAvg + (v-runningAvg)/(i+1);
            runninVariance = runninVariance+(v-oldAvg)*(v-runningAvg);
        }
        System.out.printf("Avg=%f%n",runningAvg);
        System.out.printf("StdDev=%f%n",Math.sqrt(runninVariance/size));
    }
}
