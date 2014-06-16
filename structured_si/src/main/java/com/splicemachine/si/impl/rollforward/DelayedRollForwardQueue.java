package com.splicemachine.si.impl.rollforward;

import java.util.concurrent.Executors;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.utils.SpliceLogUtils;

public class DelayedRollForwardQueue extends AbstractProcessingQueue {
	static {
		executor = Executors.newCachedThreadPool();
		disruptor = new Disruptor<RollForwardEvent>(new RollForwardEventFactory(),2048,executor,ProducerType.MULTI,new SleepingWaitStrategy());
		disruptor.handleEventsWith(new DelayedRollForwardEventHandler());
		disruptor.start();
		ringBuffer = disruptor.getRingBuffer();		
	}
	
	public DelayedRollForwardQueue(RollForwardAction action) {
		super(action);
	}
	
	public static class DelayedRollForwardEventHandler implements EventHandler<RollForwardEvent> {

		public DelayedRollForwardEventHandler() {
		}
		
		@Override
		public void onEvent(RollForwardEvent event, long sequence,boolean endOfBatch) throws Exception {
			SpliceLogUtils.trace(LOG, "on event {event=%s, sequence=%d, endOfBatch=%s",event, sequence, endOfBatch);
			event.getRollForwardAction().rollForward(event.getTransactionId(), event.getEffectiveTimestamp(), event.getRowKey());
		}
	}
}
