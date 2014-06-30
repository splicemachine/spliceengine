package com.splicemachine.si.impl;

import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.RollForwardQueueFactory;
import com.splicemachine.si.data.hbase.HbRegion;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/12/14
 */
public class NoOpRollForwardQueue implements RollForwardQueue {
		public  static final NoOpRollForwardQueue INSTANCE = new NoOpRollForwardQueue();
		public static final RollForwardQueueFactory<byte[], HbRegion> FACTORY = new RollForwardQueueFactory<byte[], HbRegion>() {
				@Override
				public RollForwardAction newAction(HbRegion table) {
						return new RollForwardAction() {
								@Override
								public Boolean rollForward(long transactionId, List<byte[]> rowList) throws IOException {
										return true;
								}
						};
				}
		};

		@Override public void start() {  }
		@Override public void stop() {  }
		@Override public void recordRow(long transactionId, byte[] rowKey, Long effectiveTimestamp) {  }
}
