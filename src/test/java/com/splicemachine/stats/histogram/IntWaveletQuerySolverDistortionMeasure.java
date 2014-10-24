package com.splicemachine.stats.histogram;

import com.carrotsearch.hppc.IntIntOpenHashMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import com.splicemachine.testutils.GaussianRandom;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Scott Fines
 * Date: 6/3/14
 */
public class IntWaveletQuerySolverDistortionMeasure {

		public static void main(String...args){
				int max = 4;
//        int max = 8;
//        int max = 64;
//				int max = 128;
//				int max = 256;
//				int max = 512;
//				int max = 65536;
//				int max = 1<<30;
				int numElements = 10;

//				List<Integer> ints = Arrays.asList(2,2,0,2,3,5,4,4);
//				List<Integer> ints = Arrays.asList(0,0,2,2,2,2,2,3,3);
//				List<Integer> ints = Arrays.asList(-8,-7,-6,-5,-4,-3,-2,-1,0,1,2,3,4,5,6,7);
//				performAnalysis(new FixedGenerator(ints.iterator()),max);

//        performAnalysis(new EnergyGenerator(numElements,new Random(0l),10,new int[]{20,15,10,17,24,30,50,80,112,90,85,95,100,105,120,133,127,110,95,85,50,44,35,27}),max);
//				performAnalysis(new UniformGenerator(numElements,max,new Random(0l)),max);
        GaussianGenerator generator = new GaussianGenerator(numElements,max,new Random(0l));
        while(generator.hasNext()){
            System.out.println(generator.next().key);
        }
//        performAnalysis(new GaussianGenerator(numElements,max,new Random(0l)),max);
		}

		protected static void performAnalysis(Iterator<IntIntPair> dataGenerator,int maxInteger) {
        int max = 2*maxInteger;
        if(max<maxInteger){
            //integer overflow
            max = Integer.MAX_VALUE;
        }
				IntIntOpenHashMap actualDistribution = new IntIntOpenHashMap();

				IntGroupedCountBuilder builder = IntGroupedCountBuilder.build(0.1f,maxInteger);

				while(dataGenerator.hasNext()){
						IntIntPair pair = dataGenerator.next();
						actualDistribution.addTo(pair.key,pair.count);
						builder.update(pair.key,pair.count);
				}

				IntWaveletQuerySolver querySolver = (IntWaveletQuerySolver)builder.build(0.0d);//build the full thingy

				long[] estimated = new long[max];
				double[] rawEstimates = new double[max];
				long[] diffs = new long[max];
				double[] relDiffs = new double[max];
				for(IntIntCursor entry:actualDistribution){
						int value = entry.key;
						int count = entry.value;

						long estimate = querySolver.equal(value);
						double rawEstimate = querySolver.estimateEquals(value);

						long diff = Math.abs(estimate-count);
						double relDiff = ((double)diff)/count;
						diffs[value+maxInteger] = diff;
						relDiffs[value+maxInteger] = relDiff;
						estimated[value+maxInteger] = estimate;
						rawEstimates[value+maxInteger] = rawEstimate;
				}

				System.out.printf("%-10s\t%10s\t%10s\t%10s\t%10s\t%10s%n", "element", "correct", "estimated", "rawEstimate", "diff", "relDiff");

				double maxRelError = 0.0d;
				int maxErrorElement = 0;
				double avgRelError = 0.0d;
				double relErrorVar = 0.0d;

				long maxAbsError = 0l;
				int maxAbsErrorElement = 0;
				for(int i=0;i<diffs.length;i++){
						int pos = i-maxInteger;
						int actual = actualDistribution.get(pos);
						long estimate = estimated[i];
						double rawEstimate = rawEstimates[i];
						long diff = diffs[i];
						double relDiff = relDiffs[i];
//						if((i&127)==0)
								System.out.printf("%-10d\t%10d\t%10d\t%10f\t%10d%10f%n",pos,actual,estimate,rawEstimate,diff,relDiff);

						if(diff>maxAbsError){
								maxAbsError = diff;
								maxAbsErrorElement = pos;
						}
						if(relDiff>maxRelError){
								maxRelError = relDiff;
								maxErrorElement = pos;
						}

						double oldAvg = avgRelError;
						avgRelError += (relDiff-avgRelError)/(i+1);
						relErrorVar += (relDiff-oldAvg)*(relDiff-avgRelError);
				}

				System.out.println("---");
				System.out.printf("Num Wavelet Coefficients: %d%n",querySolver.getNumWaveletCoefficients());

				System.out.println("---");
				System.out.printf("Max Absolute Error: %d%n",maxAbsError);
				System.out.printf("Element with Max Abs error: %d\t%d\t%d%n",maxAbsErrorElement,actualDistribution.get(maxAbsErrorElement),estimated[maxAbsErrorElement+maxInteger]);
				System.out.println("---");

				System.out.printf("Max Relative Error: %f%n", maxRelError);
				System.out.printf("Element with Max Rel. error: %d\t%d\t%d%n",maxErrorElement,actualDistribution.get(maxErrorElement),estimated[maxErrorElement+maxInteger]);
				System.out.printf("Avg Relative Error: %f%n",avgRelError);
				System.out.printf("Std. Dev: %f%n",Math.sqrt(relErrorVar/(2*maxInteger-1)));
				System.out.println("---");

				Arrays.sort(relDiffs);
				System.out.printf("p25 Rel. Error:%f%n", relDiffs[relDiffs.length / 4]);
				System.out.printf("p50 Rel. Error:%f%n", relDiffs[relDiffs.length / 2]);
				System.out.printf("p75 Rel. Error:%f%n", relDiffs[3 * relDiffs.length / 4]);
				System.out.printf("p95 Rel. Error:%f%n",relDiffs[95*relDiffs.length/100]);
				System.out.printf("p99 Rel. Error:%f%n",relDiffs[99*relDiffs.length/100]);
		}

		private static class IntIntPair {
				private int key;
				private int count;
		}

    private static class GaussianGenerator implements Iterator<IntIntPair>{
        private final GaussianRandom random;
        private final int maxInt;
        private final int numIterations;
        private int iterationCount = 0;
        private final int boundary;

        private IntIntPair retPair = new IntIntPair();
        private GaussianGenerator(int numIterations, int maxInt, Random random) {
            this.random = new GaussianRandom(random);
            this.numIterations = numIterations;
            int s = 2*maxInt;
            if(s<maxInt){
                boundary = Integer.MAX_VALUE;
                maxInt = boundary>>1;
            }else
                boundary = s;
            this.maxInt = maxInt;

        }

        @Override
        public boolean hasNext() {
            return (iterationCount++)<=numIterations;
        }

        @Override
        public IntIntPair next() {
            /*
             * random.nextDouble() generates a gaussian which is centered on 0
             * and has unit std. deviation. We want to ensure that we NEVER
             * generate numbers outside of the range (-maxInt,maxInt), so
             * we first truncate any number which >=1 (forcing us to generate
             * within the bounds [0,1). Then we scale by 2*maxInt to get to
             * [0,2*maxInt); finally, we shift down by maxInt to get to [-maxInt,maxInt)
             */
            while(true){
                double d = random.nextDouble();
                if(d>=1) continue;

                d = d*(boundary);
                if(d<0)
                    retPair.key = (int)(d)+maxInt;
                else
                    retPair.key = (int)(d)-maxInt;
                retPair.count=1;
                return retPair;
            }
        }

        @Override public void remove() { throw new UnsupportedOperationException(); }

    }
		private static class UniformGenerator implements Iterator<IntIntPair>{
				private final Random random;
				private final int maxInt;
				private final int numIterations;
				private int iterationCount = 0;
        private final int boundary;

				private IntIntPair retPair = new IntIntPair();
				private UniformGenerator(int numIterations, int maxInt, Random random) {
						this.random = random;
						this.numIterations = numIterations;
            int s = 2*maxInt;
            if(s<maxInt){
                boundary = Integer.MAX_VALUE;
                maxInt = boundary>>1;
            }else
                boundary = s;
            this.maxInt = maxInt;

				}

				@Override
				public boolean hasNext() {
						return (iterationCount++)<=numIterations;
				}

				@Override
				public IntIntPair next() {
						retPair.key = random.nextInt(2*maxInt)-maxInt;
						retPair.count = 1;
						return retPair;
				}

				@Override public void remove() { throw new UnsupportedOperationException(); }
		}

    private static class EnergyGenerator implements Iterator<IntIntPair>{
        private final int numIterations;
        private int iterationCount = 0;
        private final Random jitter;
        private final int magnitude;
        private final int[] baseValues;

        private IntIntPair retPair = new IntIntPair();
        private EnergyGenerator(int numIterations, Random jitter, int magnitude, int[] baseValues) {
            this.numIterations = numIterations;
            this.jitter = jitter;
            this.magnitude = magnitude;
            this.baseValues = baseValues;
        }

        @Override
        public boolean hasNext() {
            return (iterationCount++)<=numIterations;
        }

        @Override
        public IntIntPair next() {
            int v = baseValues[iterationCount%baseValues.length];
            if(jitter.nextBoolean()){
                v-=jitter.nextInt(magnitude);
            }else
                v+=jitter.nextInt(magnitude);
            retPair.key=v;
            retPair.count=1;
            return retPair;
        }

        @Override public void remove() { throw new UnsupportedOperationException(); }
    }

		private static class FixedGenerator implements Iterator<IntIntPair>{
				private final Iterator<Integer> valuesIterator;

				private final IntIntPair retPair = new IntIntPair();
				private FixedGenerator(Iterator<Integer> valuesIterator) {
						this.valuesIterator = valuesIterator;
						this.retPair.count=1;
				}

				@Override public boolean hasNext() { return valuesIterator.hasNext(); }

				@Override
				public IntIntPair next() {
						retPair.key = valuesIterator.next();
						return retPair;
				}

				@Override public void remove() { throw new UnsupportedOperationException(); }
		}
}
