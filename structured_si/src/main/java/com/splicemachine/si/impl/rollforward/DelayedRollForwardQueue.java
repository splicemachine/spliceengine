package com.splicemachine.si.impl.rollforward;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.utils.SpliceLogUtils;

public class DelayedRollForwardQueue extends AbstractProcessingQueue {
	protected static ExecutorService executor;
    protected static Disruptor<RollForwardEvent> disruptor;
    protected static RingBuffer<RollForwardEvent> ringBuffer;
    protected static AtomicInteger currentSize = new AtomicInteger(0);
    protected static AtomicInteger queueSize = new AtomicInteger(0);	  
    protected static AtomicLong skippedRollForwardsSize = new AtomicLong(0);	  

	static {
		executor = Executors.newCachedThreadPool();
		disruptor = new Disruptor<RollForwardEvent>(new RollForwardEventFactory(),SpliceConstants.delayedForwardRingBufferSize,executor,ProducerType.MULTI,new SleepingWaitStrategy());
		disruptor.handleEventsWith(new DelayedRollForwardEventHandler(SpliceConstants.delayedForwardWriteBufferSize, SpliceConstants.delayedForwardAsyncWriteDelay));
		disruptor.start();
		ringBuffer = disruptor.getRingBuffer();		
	}
	
	public DelayedRollForwardQueue(RollForwardAction action) {
		super(action);
	}
	
	public static class DelayedRollForwardEventHandler implements EventHandler<RollForwardEvent> {
	    public static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5,
	            new ThreadFactoryBuilder().setNameFormat("delayed-rollforward-%d").build());
	    private int timeDelayMs;
	    private final int maxEntries;
	    protected HashMap<RollForwardAction, List<RollForwardEvent>> regionMaps = new HashMap();
	    
		public DelayedRollForwardEventHandler(int maxEntries, int timeDelayMs) {
			this.maxEntries = maxEntries;
			this.timeDelayMs = timeDelayMs;
		}
		
		@Override
		public void onEvent(RollForwardEvent event, long sequence,boolean endOfBatch) throws Exception {
			SpliceLogUtils.trace(LOG, "on event {event=%s, sequence=%d, endOfBatch=%s",event, sequence, endOfBatch);
			if (currentSize.getAndIncrement()==0)
				regionMaps = new HashMap();
			currentSize.getAndIncrement();
			if (regionMaps.containsKey(event.getRollForwardAction()))
				regionMaps.get(event.getRollForwardAction()).add(event);
			else {
				List<RollForwardEvent> list = new ArrayList<RollForwardEvent>();
				list.add(event);
				regionMaps.put(event.getRollForwardAction(), list);
			}
			if (currentSize.get()>=maxEntries || endOfBatch) {
				execute(regionMaps);
				currentSize.set(0);
			}
		}
		
		private void execute(final HashMap<RollForwardAction, List<RollForwardEvent>> regionMaps) {
	        final Runnable roller = new Runnable() {
				@Override
				public void run() {
					try {
					for (RollForwardAction action: regionMaps.keySet()) {
				        if (LOG.isTraceEnabled())
				        	SpliceLogUtils.trace(LOG, "running rollForward for action=%s",action);
						action.write(regionMaps.get(action));
					}	
					} finally {
						queueSize.decrementAndGet();
					}
				}
	        };		
	        if (LOG.isTraceEnabled())
	        	SpliceLogUtils.trace(LOG, "executing scheduler on event");
			queueSize.incrementAndGet();
			if (queueSize.get() <= SpliceConstants.delayedForwardQueueLimit)
				scheduler.schedule(roller, getRollForwardDelay(), TimeUnit.MILLISECONDS);
			else
				skippedRollForwardsSize.incrementAndGet();
	    }

	    /**
	     * Provide a hook for tests to reach in and change the roll-forward delay.
	     */
	    private int getRollForwardDelay() {
	        Integer override = Tracer.rollForwardDelayOverride;
	        if (override == null) {
	            return timeDelayMs;
	        } else {
	            return override;
	        }
	    }
		
	}
	
    public static void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName coordinatorName = new ObjectName("com.splicemachine.si.impl.rollforward:type=DelayedRollForwardQueue");        
        mbs.registerMBean(DelayedRollForwardQueueReporter.get(),coordinatorName);
    }
    


		public static class DelayedRollForwardQueueReporter implements DelayedRollForwardQueueReporterIFace {
				private static final DelayedRollForwardQueueReporter INSTANCE = new DelayedRollForwardQueueReporter();
				private  DelayedRollForwardQueueReporter () {}
				public static DelayedRollForwardQueueReporter get(){ return INSTANCE; }				
				@Override public int getBufferSize() { return ringBuffer.getBufferSize(); }
				@Override public long getRemainingCapacity() { return ringBuffer.remainingCapacity(); }
				@Override public long getRollForwardQueueSize() { return queueSize.longValue(); }
				@Override public long getSkippedRollForwardsSize() { return skippedRollForwardsSize.get(); }
				@Override public long getCommittedRollForwards() { return DelayedRollForwardAction.committedRollForwards.get(); }
				@Override public long getSIFailRollForwards() { return DelayedRollForwardAction.siFailRollForwards.get(); }
				@Override public long getNotFinishedRollForwards() { return DelayedRollForwardAction.notFinishedRollForwards.get(); }

		}

		@MXBean
		@SuppressWarnings("UnusedDeclaration")
		public interface DelayedRollForwardQueueReporterIFace {
				public int getBufferSize();
				public long getRemainingCapacity();
				public long getRollForwardQueueSize();
				public long getSkippedRollForwardsSize();
				public long getCommittedRollForwards();
				public long getSIFailRollForwards();
				public long getNotFinishedRollForwards();
		}

		@Override
		Disruptor<RollForwardEvent> getDisruptor() {
			return disruptor;
		}

		@Override
		RingBuffer<RollForwardEvent> getRingBuffer() {
			return ringBuffer;
		}
	
}	