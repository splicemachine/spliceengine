package com.splicemachine.logicalstats.frequency;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.splicemachine.utils.hash.Hash32;
import com.splicemachine.utils.hash.HashFunctions;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
@SuppressWarnings("ConstantConditions")
public class FixedIntSSFrequencyCounterTest {


		@Test
		public void testWorksWithNoEviction() throws Exception {
				//insert 10 unique elements, then pull them out
				Hash32[] hashes = new Hash32[]{
								HashFunctions.murmur3(0),
								HashFunctions.murmur3(5),
								HashFunctions.murmur3(7),
								HashFunctions.murmur3(11),
								HashFunctions.murmur3(13),
				};
				IntFrequencyCounter spaceSaver = new IntSSFrequencyCounter(20,10, hashes);

				Map<Integer,Pair<Long,Long>> correctEstimates = new HashMap<Integer, Pair<Long, Long>>();
				for(int i=0;i<10;i++){
						long count = 1;
						spaceSaver.update(i);
						if(i%2==0){
								spaceSaver.update(i);
								count++;
						}
						correctEstimates.put(i,Pair.newPair(count,0l));
				}

				Set<? extends FrequencyEstimate<Integer>> estimates = spaceSaver.getFrequentElements(0f);
				Assert.assertEquals("Incorrect number of rows!", correctEstimates.size(), estimates.size());

				long totalCount = 0l;
				for(FrequencyEstimate<Integer> estimate:estimates){
						int val = estimate.value();
						long count = estimate.count();
						long error = estimate.error();
						totalCount+=count;

						Pair<Long,Long> correct = correctEstimates.get(val);
						Assert.assertNotNull("Observed entry for "+val+" not found!",correct);
						Assert.assertEquals("Incorrect count!",correct.getFirst().longValue(),count);
						Assert.assertEquals("Incorrect error!",correct.getSecond().longValue(),error);
				}
				Assert.assertEquals("Total estimated count does not equal the number of elements!",15,totalCount);
		}

		@Test
		public void testEvictsEntry() throws Exception {
				Hash32[] hashes = new Hash32[]{
								HashFunctions.murmur3(0),
								HashFunctions.murmur3(5),
								HashFunctions.murmur3(7),
								HashFunctions.murmur3(11),
								HashFunctions.murmur3(13),
				};
				IntFrequencyCounter spaceSaver = new IntSSFrequencyCounter(2,10, hashes);

				int element = 1;
				spaceSaver.update(element);
				spaceSaver.update(element);
				element = 2;
				spaceSaver.update(element);
				element = 3;
				spaceSaver.update(element);

				//output should be (1,1,0),(3,2,1)

				List<FrequencyEstimate<Integer>> estimates = Lists.newArrayList(spaceSaver.getFrequentElements(0f));
				Collections.sort(estimates,new Comparator<FrequencyEstimate<Integer>>(){
						@Override
						public int compare(FrequencyEstimate<Integer> o1, FrequencyEstimate<Integer> o2) {
								return Ints.compare(o1.value(), o2.value());
						}
				});

				List<Integer> values = Lists.transform(estimates,new Function<FrequencyEstimate<Integer>, Integer>() {
						@Override
						public Integer apply(@Nullable FrequencyEstimate<Integer> input) {
								return input.value();
						}
				});

				List<Integer> correctValues = Arrays.asList(1,3);
				Assert.assertEquals("Incorrect reported values!",correctValues,values);

				List<Long> counts = Lists.transform(estimates, new Function<FrequencyEstimate<Integer>, Long>() {
						@Override
						public Long apply(@Nullable FrequencyEstimate<Integer> input) {
								return input.count();
						}
				});
				List<Long> correctCounts = Arrays.asList(2l,2l);
				Assert.assertEquals("Incorrect reported counts!",correctCounts,counts);

				List<Long> errors = Lists.transform(estimates, new Function<FrequencyEstimate<Integer>, Long>() {
						@Override
						public Long apply(@Nullable FrequencyEstimate<Integer> input) {
								return input.error();
						}
				});

				List<Long> correctErrors = Arrays.asList(0l, 1l);
				Assert.assertEquals("Incorrect reported errors!",correctErrors,errors);
		}
}
