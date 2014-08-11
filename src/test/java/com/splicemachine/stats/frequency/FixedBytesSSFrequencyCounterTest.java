package com.splicemachine.stats.frequency;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.utils.hash.Hash32;
import com.splicemachine.utils.hash.HashFunctions;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 3/27/14
 */
@SuppressWarnings("ConstantConditions")
public class FixedBytesSSFrequencyCounterTest {

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
				BytesSSFrequencyCounter spaceSaver = new BytesSSFrequencyCounter(20,10, hashes);

				Map<byte[],Pair<Long,Long>> correctEstimates = new TreeMap<byte[], Pair<Long, Long>>(Bytes.BYTES_COMPARATOR);
				for(int i=0;i<10;i++){
						byte[] data = Bytes.toBytes(i);
						long count = 1;
						spaceSaver.update(data);
						if(i%2==0){
								spaceSaver.update(data);
								count++;
						}
						correctEstimates.put(data,Pair.newPair(count,0l));
				}

				Set<FrequencyEstimate<byte[]>> estimates = spaceSaver.getFrequentElements(0f);
				Assert.assertEquals("Incorrect number of rows!", correctEstimates.size(), estimates.size());

				long totalCount = 0l;
				for(FrequencyEstimate<byte[]> estimate:estimates){
						byte[] val = estimate.value();
						long count = estimate.count();
						long error = estimate.error();
						totalCount+=count;

						Pair<Long,Long> correct = correctEstimates.get(val);
						Assert.assertNotNull("Observed entry for "+Bytes.toInt(val)+" not found!",correct);
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
				BytesSSFrequencyCounter spaceSaver = new BytesSSFrequencyCounter(2,10, hashes);

				byte[] element = Bytes.toBytes(1);
				spaceSaver.update(element);
				spaceSaver.update(element);
				element = Bytes.toBytes(2);
				spaceSaver.update(element);
				element = Bytes.toBytes(3);
				spaceSaver.update(element);

				//output should be (1,1,0),(3,2,1)

				List<FrequencyEstimate<byte[]>> estimates = Lists.newArrayList(spaceSaver.getFrequentElements(0f));
				Collections.sort(estimates, new Comparator<FrequencyEstimate<byte[]>>() {

						@Override
						public int compare(FrequencyEstimate<byte[]> o1, FrequencyEstimate<byte[]> o2) {
								return Bytes.compareTo(o1.value(), o2.value());
						}
				});

				List<byte[]> values = Lists.transform(estimates,new Function<FrequencyEstimate<byte[]>, byte[]>() {
						@Override
						public byte[] apply(@Nullable FrequencyEstimate<byte[]> input) {
								return input.value();
						}
				});

				List<byte[]> correctValues = Arrays.asList(Bytes.toBytes(1),Bytes.toBytes(3));
				Assert.assertEquals("Incorrect reported values size!",correctValues.size(),values.size());
				for(int i=0;i<correctValues.size();i++){
						byte[] c = correctValues.get(i);
						byte[] a = values.get(i);
						Assert.assertArrayEquals("Incorrect reported value for correct value "+ Bytes.toInt(c),c,a);
				}

				List<Long> counts = Lists.transform(estimates, new Function<FrequencyEstimate<byte[]>, Long>() {
						@Override
						public Long apply(@Nullable FrequencyEstimate<byte[]> input) {
								return input.count();
						}
				});
				List<Long> correctCounts = Arrays.asList(2l,2l);
				Assert.assertEquals("Incorrect reported counts!",correctCounts,counts);

				List<Long> errors = Lists.transform(estimates,new Function<FrequencyEstimate<byte[]>, Long>() {
						@Override
						public Long apply(@Nullable FrequencyEstimate<byte[]> input) {
								return input.error();
						}
				});

				List<Long> correctErrors = Arrays.asList(0l,1l);
				Assert.assertEquals("Incorrect reported errors!",correctErrors,errors);
		}
}
