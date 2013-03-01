package com.splicemachine.si2.relations.simple;

import com.splicemachine.si2.relations.api.Clock;

public class ManualClock implements Clock {
	private long time = 0;

	public void setTime(long time) {
		this.time = time;
	}

	@Override
	public long getTime() {
		return time;
	}
}
