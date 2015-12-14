package com.splicemachine.timestamp.api;

import javax.management.MXBean;

/**
 * Interface for exposing Timestamp Generator related metrics
 * via JMX, for a region server.
 * 
 * @author Walt Koetke
 */

@MXBean
public interface TimestampClientStatistics{

	long getNumberTimestampRequests();
	
 	double getAvgTimestampRequestDuration();
	
}
