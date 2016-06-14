package com.splicemachine.stats.histogram;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author Scott Fines
 * Date: 4/1/14
 */
@RunWith(Parameterized.class)
public class EquiDepthIntHistogramTest {

		@Parameterized.Parameters
		public static Collection<Object[]> data() {
				Collection<Object[]> data = Lists.newArrayList();

				int numBuckets = 16;
				//load up 1024 monotonically increasing elements
				int size = 1024;
				IntArrayList list = IntArrayList.newInstanceWithCapacity(size);
				for(int i=0;i<size;i++){
						list.add(i);
				}
				data.add(new Object[]{list.toArray(),numBuckets});
				size = 10;
				list = IntArrayList.newInstanceWithCapacity(size);
				for(int i=0;i<size;i++){
						list.add(i);
				}
				data.add(new Object[]{list.toArray(),numBuckets});

				return data;
		}

		private final int[] data;
		private final int numBuckets;

		public EquiDepthIntHistogramTest(int[] data, int numBuckets) {
				this.data = data;
				this.numBuckets = numBuckets;
		}

		@Test
		public void testMin() throws Exception {
				int[] toAdd = Arrays.copyOf(data,data.length);

				EquiDepthIntHistogram histogram = new EquiDepthIntHistogram(toAdd,numBuckets);

				Arrays.sort(data);
				Assert.assertEquals("Incorrect minimum value!", data[0], histogram.min());
				Assert.assertEquals("Incorrect minimum value(autoboxed)!",data[0],histogram.getMin().intValue());
		}

		@Test
		public void testMax() throws Exception {
				int[] toAdd = Arrays.copyOf(data,data.length);

				EquiDepthIntHistogram histogram = new EquiDepthIntHistogram(toAdd,numBuckets);

				Arrays.sort(data);
				Assert.assertEquals("Incorrect max value!",data[data.length-1],histogram.max());
				Assert.assertEquals("Incorrect max value(autoboxed)!", data[data.length - 1], histogram.getMax().intValue());
		}

		@Test
		public void testColumns() throws Exception {
				int[] toAdd = Arrays.copyOf(data,data.length);

				EquiDepthIntHistogram histogram = new EquiDepthIntHistogram(toAdd,numBuckets);

				List<IntHistogram.IntColumn> columns = histogram.columns();
				int correctColumnCounts = Math.min(data.length,numBuckets);
				Assert.assertEquals("Incorrect number of columns!", correctColumnCounts, columns.size());
				IntHistogram.IntColumn previous;
				Iterator<IntHistogram.IntColumn> iterator = columns.iterator();
				String template = "Incorrect column structure";
				if(!iterator.hasNext()) return;
				previous = iterator.next();
				long totalCount = previous.getNumElements();
				while(iterator.hasNext()){
						IntHistogram.IntColumn current = iterator.next();
						Assert.assertTrue(String.format(template+":previous = <%s>, current=<%s>",previous,current),previous.compareTo(current)<0);
						totalCount+=current.getNumElements();
				}
				Assert.assertEquals("incorrect total count!",data.length,(int)totalCount);
		}

		@Test
		public void testGetNumBuckets() throws Exception {
				int[] toAdd = Arrays.copyOf(data,data.length);

				EquiDepthIntHistogram histogram = new EquiDepthIntHistogram(toAdd,numBuckets);

				int correctColumnCounts = Math.min(data.length,numBuckets);
				Assert.assertEquals("Incorrect number of columns!", correctColumnCounts, histogram.getNumBuckets());
		}

		@Test
		public void testAfter() throws Exception {
				int[] toAdd = Arrays.copyOf(data,data.length);

				EquiDepthIntHistogram histogram = new EquiDepthIntHistogram(toAdd,numBuckets);

				Arrays.sort(data);
				for(int i=0;i<data.length;i++){
						long estimated = histogram.after(i,false);
						int correct = data.length -getNextDifferentNumber(data,i);
						Assert.assertEquals("Incorrect estimate for number "+i,correct,estimated);
				}
		}

		@Test
		public void testAfterWithEquality() throws Exception {
				int[] toAdd = Arrays.copyOf(data,data.length);

				EquiDepthIntHistogram histogram = new EquiDepthIntHistogram(toAdd,numBuckets);

				Arrays.sort(data);
				for(int i=0;i<data.length;i++){
						long estimated = histogram.after(i,true);
						int correct = data.length -getNextDifferentNumber(data,i)+1;
						Assert.assertEquals("Incorrect estimate for number "+i,correct,estimated);
				}
		}

		private int getNextDifferentNumber(int[] data,int position) {
				int compare = data[position];
				for(int i=position;i<data.length;i++){
						if(data[i]!=compare)
								return i;
				}
				return data.length;
		}

		@Test
		public void testErrorAfter() throws Exception {

		}

		@Test
		public void testBefore() throws Exception {
				int[] toAdd = Arrays.copyOf(data,data.length);

				EquiDepthIntHistogram histogram = new EquiDepthIntHistogram(toAdd,numBuckets);

				Arrays.sort(data);
				int i=0;
				while(i<data.length){
						int value = data[i];
						long estimated = histogram.before(value,false);
						int correct = i;
						Assert.assertEquals("Incorrect before estimate!",correct,estimated);

						//skip to the first instance of the next unique number
						i = getNextDifferentNumber(data,i);
				}
		}

		@Test
		public void testBeforeWithEquality() throws Exception {
				int[] toAdd = Arrays.copyOf(data,data.length);

				EquiDepthIntHistogram histogram = new EquiDepthIntHistogram(toAdd,numBuckets);

				Arrays.sort(data);
				int i=0;
				while(i<data.length){
						int value = data[i];
						long estimated = histogram.before(value,true);
						int correct = i;
						int next = getNextDifferentNumber(data,i);
						correct+=(next-correct);
						Assert.assertEquals("Incorrect before estimate!",correct,estimated);

						//skip to the first instance of the next unique number
						i = next;
				}
		}

		@Test
		public void testErrorBefore() throws Exception {

		}

		@Test
		public void testEqual() throws Exception {
				int[] toAdd = Arrays.copyOf(data,data.length);

				EquiDepthIntHistogram histogram = new EquiDepthIntHistogram(toAdd,numBuckets);

				Arrays.sort(data);
				int i=0;
				while(i<data.length){
						int value = data[i];
						long estimated = histogram.equal(value);
						int next = getNextDifferentNumber(data,i);
						int correct=(next-i);
						Assert.assertEquals("Incorrect equals estimate!",correct,estimated);

						//skip to the first instance of the next unique number
						i = next;
				}
		}

		@Test
		public void testErrorEqual() throws Exception {

		}

		@Test
		public void testBetween() throws Exception {

		}

		@Test
		public void testErrorBetween() throws Exception {

		}

}
