package com.splicemachine.si.impl.rollforward;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.utils.SpliceLogUtils;

public class PushForwardQueue extends AbstractProcessingQueue {
	protected static ExecutorService executor;
    protected static Disruptor<RollForwardEvent> disruptor;
    protected static RingBuffer<RollForwardEvent> ringBuffer;
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
	    protected HashMap<RollForwardAction, List<RollForwardEvent>> regionMaps = new HashMap();
	    
		public PushForwardEventHandler(int maxEntries) {
			this.maxEntries = maxEntries;
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
				execute();
				currentSize.set(0);
			}
		}
		
		private void execute() {
			for (RollForwardAction action: regionMaps.keySet()) {
				action.write(regionMaps.get(action));
			}
		}
	}	
	
	   public static void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
	        ObjectName coordinatorName = new ObjectName("com.splicemachine.si.impl.rollforward:type=PushForwardQueue");
	        mbs.registerMBean(PushForwardQueueReporter.get(),coordinatorName);
	    }
	
	public static class PushForwardQueueReporter implements PushForwardQueueReporterIFace {
		private static final PushForwardQueueReporter INSTANCE = new PushForwardQueueReporter();
		private  PushForwardQueueReporter () {}
		public static PushForwardQueueReporter get(){ return INSTANCE; }
		@Override public int getBufferSize() { return ringBuffer.getBufferSize(); }
		@Override public long getRemainingCapacity() { return ringBuffer.remainingCapacity(); }
		@Override public long getPushedForwardSize() { return PushForwardAction.pushedForwardSize.get(); }		
	}

	@MXBean
	@SuppressWarnings("UnusedDeclaration")
	public interface PushForwardQueueReporterIFace {
			public int getBufferSize();
			public long getRemainingCapacity();	
			public long getPushedForwardSize();
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