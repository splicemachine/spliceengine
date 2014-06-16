package com.splicemachine.si.impl.rollforward;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.utils.SpliceLogUtils;

public class PushForwardQueue extends AbstractProcessingQueue {
	
	static {
		executor = Executors.newCachedThreadPool();
		disruptor = new Disruptor<RollForwardEvent>(new RollForwardEventFactory(),2048,executor,ProducerType.MULTI,new SleepingWaitStrategy());
		disruptor.handleEventsWith(new PushForwardEventHandler(1000));
		disruptor.start();
		ringBuffer = disruptor.getRingBuffer();		
	}
	
	public PushForwardQueue(RollForwardAction action) {
		super(action);
	}
	
	public static class PushForwardEventHandler implements EventHandler<RollForwardEvent> {
	    private final int maxEntries;
	    protected AtomicInteger currentSize = new AtomicInteger(0);

		public PushForwardEventHandler(int maxEntries) {
			this.maxEntries = maxEntries;
		}
		
		@Override
		public void onEvent(RollForwardEvent event, long sequence,boolean endOfBatch) throws Exception {
			SpliceLogUtils.trace(LOG, "on event {event=%s, sequence=%d, endOfBatch=%s",event, sequence, endOfBatch);
			if (currentSize.getAndIncrement()==0)
					event.getRollForwardAction().begin();
			event.getRollForwardAction().rollForward(event.getTransactionId(), event.getEffectiveTimestamp(), event.getRowKey());
			if (currentSize.get()>=maxEntries || endOfBatch) {
				event.getRollForwardAction().flush();
				currentSize.set(0);
				event.getRollForwardAction().begin();
			}
		}
	}	
}