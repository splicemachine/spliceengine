package com.splicemachine.si.impl;

import com.splicemachine.si.api.RollForwardQueue;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Date: 2/12/14
 */
public class NoOpRollForwardQueue implements RollForwardQueue<byte[],ByteBuffer> {
		public  static final NoOpRollForwardQueue INSTANCE = new NoOpRollForwardQueue();
		@Override public void start() {  }
		@Override public void stop() {  }
		@Override public void recordRow(long transactionId, byte[] rowKey, Boolean knownToBeCommitted) {  }
		@Override public int getCount() { return 0; }
}
