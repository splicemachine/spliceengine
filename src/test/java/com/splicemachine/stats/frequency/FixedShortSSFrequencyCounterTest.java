package com.splicemachine.stats.frequency;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
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
public class FixedShortSSFrequencyCounterTest {
		@Test
		public void testWorksWithNoEviction() throws Exception {
				//insert 10 unique elements, then pull them out
				ShortFrequencyCounter spaceSaver = new ShortSSFrequencyCounter(20,10, HashFunctions.murmur3(0));

				Map<Long,LongPair> correctEstimates = new HashMap<Long, LongPair>();
				for(short i=0;i<10;i++){
						long count = 1;
						spaceSaver.update(i);
						if(i%2==0){
								spaceSaver.update(i);
								count++;
						}
						correctEstimates.put((long) i,new LongPair(count,0l));
				}

				Set<? extends FrequencyEstimate<Short>> estimates = spaceSaver.getFrequentElements(0f);
				Assert.assertEquals("Incorrect number of rows!", correctEstimates.size(), estimates.size());

				long totalCount = 0l;
				for(FrequencyEstimate<Short> estimate:estimates){
						long val = estimate.value();
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
				ShortFrequencyCounter spaceSaver = new ShortSSFrequencyCounter(2,10, HashFunctions.murmur3(0));

				short element = 1;
				spaceSaver.update(element);
				spaceSaver.update(element);
				element = 2;
				spaceSaver.update(element);
				element = 3;
				spaceSaver.update(element);

				//output should be (1,1,0),(3,2,1)

				List<FrequencyEstimate<Short>> estimates = Lists.newArrayList(spaceSaver.getFrequentElements(0f));
				Collections.sort(estimates, new Comparator<FrequencyEstimate<Short>>() {

						@Override
						public int compare(FrequencyEstimate<Short> o1, FrequencyEstimate<Short> o2) {
								return Longs.compare(o1.value(), o2.value());
						}
				});

				List<Short> values = Lists.transform(estimates,new Function<FrequencyEstimate<Short>, Short>() {
						@Override
						public Short apply(@Nullable FrequencyEstimate<Short> input) {
								return input.value();
						}
				});

				List<Short> correctValues = Arrays.asList((short)1,(short)3);
				Assert.assertEquals("Incorrect reported values!",correctValues,values);

				List<Long> counts = Lists.transform(estimates, new Function<FrequencyEstimate<Short>, Long>() {
						@Override
						public Long apply(@Nullable FrequencyEstimate<Short> input) {
								return input.count();
						}
				});
				List<Long> correctCounts = Arrays.asList(2l,2l);
				Assert.assertEquals("Incorrect reported counts!",correctCounts,counts);

				List<Long> errors = Lists.transform(estimates,new Function<FrequencyEstimate<Short>, Long>() {
						@Override
						public Long apply(@Nullable FrequencyEstimate<Short> input) {
								return input.error();
						}
				});

				List<Long> correctErrors = Arrays.asList(0l,1l);
				Assert.assertEquals("Incorrect reported errors!",correctErrors,errors);
		}
}
