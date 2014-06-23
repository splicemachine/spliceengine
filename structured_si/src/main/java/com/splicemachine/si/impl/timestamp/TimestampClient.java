package com.splicemachine.si.impl.timestamp;

/**
 * Client interface for the generation of unique monotonically increasing timestamps,
 * as needed by the transaction system.
 */
public abstract class TimestampClient extends TimestampBaseHandler {

	/**
	 * Produces the next timestamp.
	 * 
	 * @return the next timestamp as a long
	 */
	public abstract long getNextTimestamp();
	
}
