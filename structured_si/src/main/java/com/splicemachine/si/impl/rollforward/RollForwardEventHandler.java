package com.splicemachine.si.impl.rollforward;

import org.apache.log4j.Logger;

import com.lmax.disruptor.EventHandler;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.utils.SpliceLogUtils;

public class RollForwardEventHandler implements EventHandler<RollForwardEvent> {
    private static Logger LOG = Logger.getLogger(RollForwardEventHandler.class);
	RollForwardAction action;

	public RollForwardEventHandler() {
	}

	public RollForwardEventHandler(RollForwardAction action) {
		this.action = action;
	}
	
	@Override
	public void onEvent(RollForwardEvent event, long sequence,boolean endOfBatch) throws Exception {
		SpliceLogUtils.trace(LOG, "on event {event=%s, sequence=%d, endOfBatch=%s",event, sequence, endOfBatch);
		
	}

}
