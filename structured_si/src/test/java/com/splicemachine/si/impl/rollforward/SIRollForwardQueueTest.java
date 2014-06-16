package com.splicemachine.si.impl.rollforward;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class SIRollForwardQueueTest {

	@Test
	public void testRollForward() {
		SIRollForwardQueue rollForwardQueue = new SIRollForwardQueue(null);
		rollForwardQueue.start();
		for (int i = 0; i< 1000000; i++) {
			rollForwardQueue.recordRow(i, Bytes.toBytes(i), new Long(i));
		}		
		rollForwardQueue.stop();
		
	}
	
}
