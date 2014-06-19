package com.splicemachine.si.impl.rollforward;

import org.apache.log4j.Logger;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.utils.SpliceLogUtils;

public class SIRollForwardQueue implements RollForwardQueue {
    private static Logger LOG = Logger.getLogger(SIRollForwardQueue.class);
	public RollForwardQueue delayedRollForwardQueue;
	public RollForwardQueue pushForwardQueue;
	
	public SIRollForwardQueue(RollForwardAction delayedRollForwardQueueAction, RollForwardAction pushForwardQueueAction) {
		delayedRollForwardQueue = new DelayedRollForwardQueue(delayedRollForwardQueueAction);
		pushForwardQueue = new PushForwardQueue(pushForwardQueueAction);
	}

	public SIRollForwardQueue(RollForwardQueue delayedRollForwardQueue, RollForwardQueue pushForwardQueue) {
		this.delayedRollForwardQueue = delayedRollForwardQueue;
		this.pushForwardQueue = pushForwardQueue;
	}

	
	@Override
	public void start() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.debug(LOG, "SIRollForwardQueue started");
	}

	@Override
	public void stop() {
	}

	@Override
	public void recordRow(long transactionId, byte[] rowKey,Long effectiveTimestamp) {
		if (effectiveTimestamp != null)
			pushForwardQueue.recordRow(transactionId, rowKey, effectiveTimestamp);
		else
			delayedRollForwardQueue.recordRow(transactionId, rowKey, effectiveTimestamp);
    }

}
