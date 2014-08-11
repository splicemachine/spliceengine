package com.splicemachine.stats.cardinality;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.splicemachine.stats.IntUpdateable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Random;

/**
 * @author Scott Fines
 * Date: 3/27/14
 */
public class CardinalityTest {


		private CardinalityTest(){}

		/**
		 *
		 * @param estimator the estimator to use
		 * @param numElements the total number of elements to generate
		 * @param numDistinctElements the number of distinct elements to generate
		 * @return the relative error in the estimate. relative error = Math.abs(actual-expected)/expected
		 */
		public static double test(BaseLogLogCounter estimator, int numElements, int numDistinctElements,Random random){
				IntOpenHashSet actualDistinct = IntOpenHashSet.newInstance();
				if(estimator instanceof IntUpdateable)
						fill((IntUpdateable)estimator,numElements,numDistinctElements,random,actualDistinct);
				else
						fill(estimator, numElements, numDistinctElements, random,actualDistinct);

				long cardinalityEstimate = estimator.getEstimate();
				long actualDistinctCount = actualDistinct.size();
				long absoluteError = Math.abs(cardinalityEstimate-actualDistinctCount);
				return ((double)absoluteError)/actualDistinctCount;
		}

		protected static void fill(BaseLogLogCounter estimator, int numElements, int numDistinctElements, Random random,IntOpenHashSet actualDistinct) {
				int numFillElements = random.nextInt(numElements);
				for(int i=0;i<numFillElements;i++){
						int next = i % numDistinctElements;
						actualDistinct.add(next);
						byte[] data = Bytes.toBytes(next);
						estimator.update(data, 0, data.length);
				}
		}

		protected static void fill(IntUpdateable estimator, int numElements, int numDistinctElements, Random random,IntOpenHashSet actualDistinct) {
				int numElementsToFill = random.nextInt(numElements);
				for(int i=0;i<numElementsToFill;i++){
						int next = i % numDistinctElements;

						actualDistinct.add(next);
						estimator.update(next);
				}
		}

		/**
		 *
		 * @param estimator the estimator to use
		 * @param numElements the total number of elements to generate
		 * @param numDistinctElements the number of distinct elements to generate
		 * @return the relative error in the estimate. relative error = Math.abs(actual-expected)/expected
		 */
		public static double test(BaseLogLogCounter estimator, int numElements, int numDistinctElements){
				return test(estimator,numElements,numDistinctElements,new Random(System.currentTimeMillis()));
		}
}
