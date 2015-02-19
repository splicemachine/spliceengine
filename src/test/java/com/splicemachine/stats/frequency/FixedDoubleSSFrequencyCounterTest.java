package com.splicemachine.stats.frequency;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.stats.LongPair;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
@SuppressWarnings("ConstantConditions")
public class FixedDoubleSSFrequencyCounterTest {
		@Test
		public void testWorksWithNoEviction() throws Exception {
				//insert 10 unique elements, then pull them out
				DoubleFrequencyCounter spaceSaver = FrequencyCounters.doubleCounter(20,10);//new DoubleSSFrequencyCounter(20,10, HashFunctions.murmur3(0));

				Map<Double,LongPair> correctEstimates = new HashMap<Double, LongPair>();
				for(int i=0;i<10;i++){
						long count = 1;
						spaceSaver.update(i);
						if(i%2==0){
								spaceSaver.update(i);
								count++;
						}
						correctEstimates.put((double) i, new LongPair(count,0l));
				}

        DoubleFrequentElements estimates = spaceSaver.heavyHitters(0f);
        Assert.fail("IMPLEMENT");
//        Set<? extends FrequencyEstimate<Double>> estimates = spaceSaver.getFrequentElements(0f);
//				Assert.assertEquals("Incorrect number of rows!", correctEstimates.size(), estimates.size());
//
//				long totalCount = 0l;
//				for(FrequencyEstimate<Double> estimate:estimates){
//						double val = estimate.getValue();
//						long count = estimate.count();
//						long error = estimate.error();
//						totalCount+=count;
//
//						LongPair correct = correctEstimates.get(val);
//						Assert.assertNotNull("Observed entry for "+val+" not found!",correct);
//						Assert.assertEquals("Incorrect count!",correct.getFirst(),count);
//						Assert.assertEquals("Incorrect error!",correct.getSecond(),error);
//				}
//				Assert.assertEquals("Total estimated count does not equal the number of elements!",15,totalCount);
		}

		@Test
		public void testEvictsEntry() throws Exception {
				DoubleFrequencyCounter spaceSaver = FrequencyCounters.doubleCounter(2,10);//new DoubleSSFrequencyCounter(2,10, HashFunctions.murmur3(0));

				long element = 1;
				spaceSaver.update(element);
				spaceSaver.update(element);
				element = 2;
				spaceSaver.update(element);
				element = 3;
				spaceSaver.update(element);

				//output should be (1,1,0),(3,2,1)

        DoubleFrequentElements estimates = spaceSaver.heavyHitters(0f);
//				List<FrequencyEstimate<Double>> estimates = Lists.newArrayList(spaceSaver.getFrequentElements(0f));
        Assert.fail("IMPLEMENT");
//				Collections.sort(estimates, new Comparator<FrequencyEstimate<Double>>() {
//
//						@Override
//						public int compare(FrequencyEstimate<Double> o1, FrequencyEstimate<Double> o2) {
//								return Double.compare(o1.getValue(), o2.getValue());
//						}
//				});
//
//				List<Double> values = Lists.transform(estimates,new Function<FrequencyEstimate<Double>, Double>() {
//						@Override
//						public Double apply(@Nullable FrequencyEstimate<Double> input) {
//								return input.getValue();
//						}
//				});
//
//				List<Double> correctValues = Arrays.asList(1d,3d);
//				Assert.assertEquals("Incorrect reported values!",correctValues,values);
//
//				List<Long> counts = Lists.transform(estimates, new Function<FrequencyEstimate<Double>, Long>() {
//						@Override
//						public Long apply(@Nullable FrequencyEstimate<Double> input) {
//								return input.count();
//						}
//				});
//				List<Long> correctCounts = Arrays.asList(2l,2l);
//				Assert.assertEquals("Incorrect reported counts!",correctCounts,counts);
//
//				List<Long> errors = Lists.transform(estimates,new Function<FrequencyEstimate<Double>, Long>() {
//						@Override
//						public Long apply(@Nullable FrequencyEstimate<Double> input) {
//								return input.error();
//						}
//				});
//
//				List<Long> correctErrors = Arrays.asList(0l,1l);
//				Assert.assertEquals("Incorrect reported errors!",correctErrors,errors);
		}
}
