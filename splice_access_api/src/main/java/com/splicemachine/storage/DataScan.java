package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public interface DataScan extends Attributable{

    DataScan startKey(byte[] startKey);

    DataScan stopKey(byte[] stopKey);

    DataScan filter(DataFilter df);

    /**
     * Reverse the order in which this scan is operating.
     *
     * @return a scan which scans in reverse (i.e. descending order).
     */
    DataScan reverseOrder();

    boolean isDescendingScan();

    DataScan cacheRows(int rowsToCache);

    DataScan batchCells(int cellsToBatch);

    byte[] getStartKey();

    byte[] getStopKey();

    long highVersion();

    long lowVersion();

    DataFilter getFilter();

    void setTimeRange(long lowVersion,long highVersion);

    void returnAllVersions();


}
