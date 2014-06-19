package com.splicemachine.si.impl.rollforward;

import com.lmax.disruptor.EventFactory;
/**
 * Used to prime the disruptor's ring buffer.
 * 
 *
 */
public class RollForwardEventFactory implements EventFactory<RollForwardEvent> {

	@Override
	public RollForwardEvent newInstance() {
		return new RollForwardEvent();
	}

}
