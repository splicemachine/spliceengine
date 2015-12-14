package com.splicemachine.timestamp.api;

public interface Callback {
	void error(Exception e);

	void complete(long timestamp);
}
