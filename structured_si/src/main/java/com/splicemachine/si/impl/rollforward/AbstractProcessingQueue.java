package com.splicemachine.si.impl.rollforward;

import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import com.lmax.disruptor.EventTranslatorVararg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class AbstractProcessingQueue implements RollForwardQueue {
    protected static Logger LOG = Logger.getLogger(SIRollForwardQueue.class);
    protected RollForwardAction action;

    public AbstractProcessingQueue(RollForwardAction action) {
    	this.action = action;
    }
    
    abstract Disruptor<RollForwardEvent> getDisruptor();
    abstract RingBuffer<RollForwardEvent> getRingBuffer();

    
	@Override
	public void start() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.debug(LOG, "PushForward started");
	}
	
	@Override
	public void stop() {
		try {
			getDisruptor().shutdown(2, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void recordRow(long transactionId, byte[] rowKey,Long effectiveTimestamp) {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "recordRow {transactionId=%d, rowKey=%s, effectiveTimestamp=%d}",transactionId, rowKey,effectiveTimestamp);
        getRingBuffer().publishEvent(TRANSLATOR,action,transactionId, rowKey, effectiveTimestamp);
    }
		
	 private static final EventTranslatorVararg<RollForwardEvent> TRANSLATOR =
			 new EventTranslatorVararg<RollForwardEvent>() {

					@Override
					public void translateTo(RollForwardEvent event,
							long sequence, Object... args) {
						event.set(args[0], args[1], args[2], args[3]);
					}
		        };		        
}
