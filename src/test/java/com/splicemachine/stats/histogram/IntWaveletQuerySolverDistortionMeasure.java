package com.splicemachine.stats.histogram;

import com.carrotsearch.hppc.IntIntOpenHashMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Scott Fines
 * Date: 6/3/14
 */
public class IntWaveletQuerySolverDistortionMeasure {

		public static void main(String...args){
//				int max = 8;
//				int max = 128;
//				int max = 256;
				int max = 512;
//				int max = 65536;
//				int max = 131072;
				int numElements = 100000;

//				List<Integer> ints = Arrays.asList(2,2,0,2,3,5,4,4);
//				List<Integer> ints = Arrays.asList(0,0,2,2,2,2,2,3,3);
//				List<Integer> ints = Arrays.asList(-8,-7,-6,-5,-4,-3,-2,-1,0,1,2,3,4,5,6,7);
//				performAnalysis(new FixedGenerator(ints.iterator()),max);

				performAnalysis(new RandomGenerator(numElements,max,new Random()),max);
		}

		protected static void performAnalysis(Iterator<IntIntPair> dataGenerator,int maxInteger) {
				IntIntOpenHashMap actualDistribution = new IntIntOpenHashMap(2*maxInteger);

				IntGroupedCountBuilder builder = IntGroupedCountBuilder.build(0.1f,maxInteger);

				while(dataGenerator.hasNext()){
						IntIntPair pair = dataGenerator.next();
						actualDistribution.addTo(pair.key,pair.count);
						builder.update(pair.key,pair.count);
				}

				IntWaveletQuerySolver querySolver = (IntWaveletQuerySolver)builder.build(0.2d);//build the full thingy

				long[] estimated = new long[2*maxInteger];
				double[] rawEstimates = new double[2*maxInteger];
				long[] diffs = new long[2*maxInteger];
				double[] relDiffs = new double[2*maxInteger];
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
						if((i&127)==0)
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

		private static class RandomGenerator implements Iterator<IntIntPair>{
				private final Random random;
				private final int maxInt;
				private final int numIterations;
				private int iterationCount = 0;

				private IntIntPair retPair = new IntIntPair();
				private RandomGenerator(int numIterations,int maxInt, Random random) {
						this.random = random;
						this.numIterations = numIterations;
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
