package com.splicemachine.si.impl.rollforward;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class AbstractProcessingQueue implements RollForwardQueue {
    protected static Logger LOG = Logger.getLogger(SIRollForwardQueue.class);
    protected static Executor executor;
    protected static Disruptor<RollForwardEvent> disruptor;
    protected static RingBuffer<RollForwardEvent> ringBuffer;
    protected RollForwardAction action;

    public AbstractProcessingQueue(RollForwardAction action) {
    	this.action = action;
    }
	    
	@Override
	public void start() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.debug(LOG, "PushForward started");
	}
	
	@Override
	public void stop() {
		try {
			disruptor.shutdown(2, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void recordRow(long transactionId, byte[] rowKey,Long effectiveTimestamp) {
        long sequence = ringBuffer.next();
        try {
        	RollForwardEvent event = ringBuffer.get(sequence); 
            event.setData(action, transactionId, rowKey, effectiveTimestamp);
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
    }

	@Override
	public int getCount() {
		return 0;
	}
}
