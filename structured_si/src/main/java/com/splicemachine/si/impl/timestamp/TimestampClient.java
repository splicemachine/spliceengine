package com.splicemachine.si.impl.timestamp;

public abstract class TimestampClient extends TimestampBaseHandler {

	public abstract long getNextTimestamp();
	
}
