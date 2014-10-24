package com.splicemachine.stats.frequency;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.hash.Hash32;
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
public class FixedStringSSFrequencyCounterTest {
		@Test
		public void testWorksWithNoEviction() throws Exception {
				//insert 10 unique elements, then pull them out
				SSFrequencyCounter<String> spaceSaver = new SSFrequencyCounter<String>(20,10,HashFunctions.murmur3(0));

				Map<String,LongPair> correctEstimates = new HashMap<String, LongPair>();
				for(int i=0;i<10;i++){
						long count = 1;
						spaceSaver.update(Integer.toString(i));
						if(i%2==0){
								spaceSaver.update(Integer.toString(i));
								count++;
						}
						correctEstimates.put(Integer.toString(i),new LongPair(count,0l));
				}

				Set<FrequencyEstimate<String>> estimates = spaceSaver.getFrequentElements(0f);
				Assert.assertEquals("Incorrect number of rows!", correctEstimates.size(), estimates.size());

				long totalCount = 0l;
				for(FrequencyEstimate<String> estimate:estimates){
						String val = estimate.value();
						long count = estimate.count();
						long error = estimate.error();
						totalCount+=count;

						LongPair correct = correctEstimates.get(val);
						Assert.assertNotNull("Observed entry for "+val+" not found!",correct);
						Assert.assertEquals("Incorrect count!",correct.getFirst(),count);
						Assert.assertEquals("Incorrect error!",correct.getSecond(),error);
				}
				Assert.assertEquals("Total estimated count does not equal the number of elements!",15,totalCount);
		}

		@Test
		public void testEvictsEntry() throws Exception {
				SSFrequencyCounter<String> spaceSaver = new SSFrequencyCounter<String>(2,10, HashFunctions.murmur3(0));

				String element = "1";
				spaceSaver.update(element);
				spaceSaver.update(element);
				element = "2";
				spaceSaver.update(element);
				element = "3";
				spaceSaver.update(element);

				//output should be (1,1,0),(3,2,1)

				List<FrequencyEstimate<String>> estimates = Lists.newArrayList(spaceSaver.getFrequentElements(0f));
				Collections.sort(estimates, new Comparator<FrequencyEstimate<String>>() {

						@Override
						public int compare(FrequencyEstimate<String> o1, FrequencyEstimate<String> o2) {
								return o1.value().compareTo(o2.value());
						}
				});

				List<String> values = Lists.transform(estimates,new Function<FrequencyEstimate<String>, String>() {
						@Override
						public String apply(@Nullable FrequencyEstimate<String> input) {
								return input.value();
						}
				});

				List<String> correctValues = Arrays.asList("1","3");
				Assert.assertEquals("Incorrect reported values!",correctValues,values);

				List<Long> counts = Lists.transform(estimates, new Function<FrequencyEstimate<String>, Long>() {
						@Override
						public Long apply(@Nullable FrequencyEstimate<String> input) {
								return input.count();
						}
				});
				List<Long> correctCounts = Arrays.asList(2l,2l);
				Assert.assertEquals("Incorrect reported counts!",correctCounts,counts);

				List<Long> errors = Lists.transform(estimates,new Function<FrequencyEstimate<String>, Long>() {
						@Override
						public Long apply(@Nullable FrequencyEstimate<String> input) {
								return input.error();
						}
				});

				List<Long> correctErrors = Arrays.asList(0l,1l);
				Assert.assertEquals("Incorrect reported errors!",correctErrors,errors);
		}
}
