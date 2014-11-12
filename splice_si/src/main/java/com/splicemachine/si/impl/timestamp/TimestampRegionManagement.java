package com.splicemachine.si.impl.timestamp;

import javax.management.MXBean;

/**
 * Interface for exposing Timestamp Generator related metrics
 * via JMX, for a region server.
 * 
 * @author Walt Koetke
 */

@MXBean
public interface TimestampRegionManagement {

	public long getNumberTimestampRequests();
	
 	public double getAvgTimestampRequestDuration();
	
}
