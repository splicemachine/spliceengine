package com.splicemachine.si.impl;

import com.splicemachine.si.api.RollForwardQueue;

/**
 * @author Scott Fines
 *         Date: 2/12/14
 */
public class NoOpRollForwardQueue implements RollForwardQueue {
		public  static final NoOpRollForwardQueue INSTANCE = new NoOpRollForwardQueue();
		@Override public void start() {  }
		@Override public void stop() {  }
		@Override public void recordRow(long transactionId, byte[] rowKey, Long effectiveTimestamp) {  }
}
