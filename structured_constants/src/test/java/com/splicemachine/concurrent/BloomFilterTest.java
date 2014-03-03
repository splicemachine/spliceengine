package com.splicemachine.concurrent;

import java.nio.ByteBuffer;
import org.junit.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class BloomFilterTest {

	@Test
	public void testBloom() {
		BloomFilter filter = BloomFilter.getFilter(5000, 0.01);
		for (int i = 0; i<100000;i++) {
			filter.add(ByteBuffer.wrap(Bytes.toBytes("sfsfdsfdsf" + i + "dfgdgf" + i)));
		}
		int hit = 0;
		int miss = 0;
		for (int i = 0; i<100000;i++) {
			if (filter.isPresent(ByteBuffer.wrap(Bytes.toBytes("sfsfdsfdsf" + i + "dfgdgf" + i)))) 
				hit++;
			else 
				miss++;
		}
		Assert.assertTrue("We had a false negative",hit==100000);
	}
	
}
