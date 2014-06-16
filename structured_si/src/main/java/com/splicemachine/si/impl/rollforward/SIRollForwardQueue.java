package com.splicemachine.si.impl.rollforward;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.utils.SpliceLogUtils;

public class SIRollForwardQueue implements RollForwardQueue {
    private static Logger LOG = Logger.getLogger(SIRollForwardQueue.class);
	private static Executor executor;
	private static Disruptor disruptor;
	private RollForwardAction action;
	private static RingBuffer<RollForwardEvent> ringBuffer;
	
	public SIRollForwardQueue(RollForwardAction action) {
		this.action = action;
	}
	
	static {
		executor = Executors.newCachedThreadPool();
		disruptor = new Disruptor<RollForwardEvent>(new RollForwardEventFactory(),2048,executor,ProducerType.MULTI,new SleepingWaitStrategy());
		disruptor.handleEventsWith(new RollForwardEventHandler());
		disruptor.start();
		ringBuffer = disruptor.getRingBuffer();		
	}
	
	@Override
	public void start() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.debug(LOG, "SIRollForwardQueue started");
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
		// TODO Auto-generated method stub
		return 0;
	}

}
