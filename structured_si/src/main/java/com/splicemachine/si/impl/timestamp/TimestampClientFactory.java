package com.splicemachine.si.impl.timestamp;

import com.splicemachine.constants.SpliceConstants;

public class TimestampClientFactory {

	public static TimestampClient createNewInstance() {
		return new TimestampClientMapImpl(SpliceConstants.timestampServerBindPort);
	}
	
}
