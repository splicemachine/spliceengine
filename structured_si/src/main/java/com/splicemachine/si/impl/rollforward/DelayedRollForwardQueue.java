package com.splicemachine.si.impl.rollforward;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.utils.SpliceLogUtils;

public class DelayedRollForwardQueue extends AbstractProcessingQueue {
	static {
		executor = Executors.newCachedThreadPool();
		disruptor = new Disruptor<RollForwardEvent>(new RollForwardEventFactory(),2048,executor,ProducerType.MULTI,new SleepingWaitStrategy());
		disruptor.handleEventsWith(new DelayedRollForwardEventHandler(1000, 5000));
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
	    protected AtomicInteger currentSize = new AtomicInteger(0);
	    protected HashMap<RollForwardAction, List<RollForwardEvent>> regionMaps = new HashMap();
	    
		public DelayedRollForwardEventHandler(int maxEntries, int timeDelayMs) {
			this.maxEntries = maxEntries;
			this.timeDelayMs = timeDelayMs;
		}
		
		@Override
		public void onEvent(RollForwardEvent event, long sequence,boolean endOfBatch) throws Exception {
			SpliceLogUtils.trace(LOG, "on event {event=%s, sequence=%d, endOfBatch=%s",event, sequence, endOfBatch);
			if (currentSize.getAndIncrement()==0)
				regionMaps.clear();
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
			}
		}
		
		private void execute(final HashMap<RollForwardAction, List<RollForwardEvent>> regionMaps) {
	        final Runnable roller = new Runnable() {
				@Override
				public void run() {
					for (RollForwardAction action: regionMaps.keySet()) {
						action.write(regionMaps.get(action));
					}					
				}
	        };		
	     scheduler.schedule(roller, getRollForwardDelay(), TimeUnit.MILLISECONDS);
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
        ObjectName coordinatorName = new ObjectName("com.splicemachine.si.impl.rollforward=DelayedRollForwardQueue");
        mbs.registerMBean(DelayedRollForwardQueueReporter.get(),coordinatorName);
    }

		public static class DelayedRollForwardQueueReporter implements DelayedRollForwardQueueReporterIFace {
				private static final DelayedRollForwardQueueReporter INSTANCE = new DelayedRollForwardQueueReporter();
				private  DelayedRollForwardQueueReporter () {}

				public static DelayedRollForwardQueueReporter get(){ return INSTANCE; }
				
				@Override public int getBufferSize() { return ringBuffer.getBufferSize(); }
				@Override public long getRemainingCapacity() { return ringBuffer.remainingCapacity(); }
				
		}

		@MXBean
		@SuppressWarnings("UnusedDeclaration")
		public interface DelayedRollForwardQueueReporterIFace {
				public int getBufferSize();
				public long getRemainingCapacity();		
		}
	
}	