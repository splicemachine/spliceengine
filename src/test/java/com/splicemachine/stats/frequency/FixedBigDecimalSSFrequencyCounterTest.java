package com.splicemachine.stats.frequency;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.stats.LongPair;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
@SuppressWarnings("ConstantConditions")
public class FixedBigDecimalSSFrequencyCounterTest {
		@Test
		public void testWorksWithNoEviction() throws Exception {
				//insert 10 unique elements, then pull them out
				FrequencyCounter<BigDecimal> spaceSaver = FrequencyCounters.counter(20,10);

				Map<BigDecimal,LongPair> correctEstimates = new HashMap<BigDecimal, LongPair>();
				for(int i=0;i<10;i++){
						long count = 1;
						BigDecimal item = BigDecimal.valueOf(i);
						spaceSaver.update(item);
						if(i%2==0){
								spaceSaver.update(item);
								count++;
						}
						correctEstimates.put(item, new LongPair(count, 0l));
				}

        FrequentElements<BigDecimal> estimates = spaceSaver.heavyHitters(0f);
        Assert.fail("IMPLEMENT");
//				Set<FrequencyEstimate<BigDecimal>> estimates = spaceSaver.getFrequentElements(0f);
//				Assert.assertEquals("Incorrect number of rows!", correctEstimates.size(), estimates.);
//
//				long totalCount = 0l;
//				for(FrequencyEstimate<BigDecimal> estimate:estimates){
//						BigDecimal val = estimate.getValue();
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
				FrequencyCounter<BigDecimal> spaceSaver = FrequencyCounters.counter(2,10);

				BigDecimal element = BigDecimal.valueOf(1);
				spaceSaver.update(element);
				spaceSaver.update(element);
				element = BigDecimal.valueOf(2);
				spaceSaver.update(element);
				element = BigDecimal.valueOf(3);
				spaceSaver.update(element);

				//output should be (1,1,0),(3,2,1)

        Assert.fail("IMPLEMENT");
//				List<FrequencyEstimate<BigDecimal>> estimates = Lists.newArrayList(spaceSaver.getFrequentElements(0f));
//				Collections.sort(estimates, new Comparator<FrequencyEstimate<BigDecimal>>() {
//
//						@Override
//						public int compare(FrequencyEstimate<BigDecimal> o1, FrequencyEstimate<BigDecimal> o2) {
//								return o1.getValue().compareTo(o2.getValue());
//						}
//				});
//
//				List<BigDecimal> values = Lists.transform(estimates,new Function<FrequencyEstimate<BigDecimal>, BigDecimal>() {
//						@Override
//						public BigDecimal apply(@Nullable FrequencyEstimate<BigDecimal> input) {
//								return input.getValue();
//						}
//				});
//
//				List<BigDecimal> correctValues = Arrays.asList(BigDecimal.ONE,BigDecimal.valueOf(3));
//				Assert.assertEquals("Incorrect reported values!",correctValues,values);
//
//				List<Long> counts = Lists.transform(estimates, new Function<FrequencyEstimate<BigDecimal>, Long>() {
//						@Override
//						public Long apply(@Nullable FrequencyEstimate<BigDecimal> input) {
//								return input.count();
//						}
//				});
//				List<Long> correctCounts = Arrays.asList(2l,2l);
//				Assert.assertEquals("Incorrect reported counts!",correctCounts,counts);
//
//				List<Long> errors = Lists.transform(estimates,new Function<FrequencyEstimate<BigDecimal>, Long>() {
//						@Override
//						public Long apply(@Nullable FrequencyEstimate<BigDecimal> input) {
//								return input.error();
//						}
//				});
//
//				List<Long> correctErrors = Arrays.asList(0l,1l);
//				Assert.assertEquals("Incorrect reported errors!",correctErrors,errors);
		}
}
