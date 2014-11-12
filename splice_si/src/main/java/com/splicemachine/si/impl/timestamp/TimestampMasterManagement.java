package com.splicemachine.si.impl.timestamp;

import javax.management.MXBean;

/**
 * Interface for exposing Timestamp Generator related metrics
 * via JMX, for the master server.
 * 
 * @author Walt Koetke
 */

@MXBean
public interface TimestampMasterManagement {

    /**
     * Returns the total number of timestamps that have been created
     * since the last master server start.
     * 
     * @return number of timestamps created
     */
	public long getNumberTimestampsCreated();
	
    /**
     * Returns the total number of timestamp 'blocks' reserved
     * since the last master server start.
     * 
     * @return number of timestamp blocks reserved
     */
	public long getNumberBlocksReserved();
	
}
