package com.splicemachine.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Stats;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.security.Key;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 1/21/14
 */
public class BufferedRegionScannerTest {

		@Test
		public void testCanReadBeyondMaxBufferSize() throws Exception {
				HRegion region = mock(HRegion.class);

				RegionScanner fromListScanner = mock(RegionScanner.class);
				List<KeyValue> dataList = Lists.newArrayListWithCapacity(4);
				dataList.add(new KeyValue(new byte[]{0},new byte[]{0},new byte[]{0},new byte[]{0}));
				dataList.add(new KeyValue(new byte[]{1},new byte[]{0},new byte[]{0},new byte[]{0}));
				dataList.add(new KeyValue(new byte[]{2},new byte[]{0},new byte[]{0},new byte[]{0}));
				dataList.add(new KeyValue(new byte[]{3},new byte[]{0},new byte[]{0},new byte[]{0}));

				final LinkedList<KeyValue> toRetList = Lists.newLinkedList(dataList);
				when(fromListScanner.nextRaw(any(List.class),any(String.class))).thenAnswer(new Answer<Boolean>() {
						@Override
						public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
								List<KeyValue> destList = (List<KeyValue>) invocationOnMock.getArguments()[0];
								if(toRetList.size()>0){
										destList.add(toRetList.removeFirst());
										return true;
								}else return false;
						}
				});
				MetricFactory factory = Stats.noOpMetricFactory();
				BufferedRegionScanner scanner = new BufferedRegionScanner(region,fromListScanner,2,1024,factory);

				int count =0;
				while(scanner.nextRaw(new ArrayList<KeyValue>(),null))
						count++;

				Assert.assertEquals("incorrect number of rows returned!",dataList.size(),count);
		}
}
