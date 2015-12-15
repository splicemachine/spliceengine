package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public interface DataGet extends Attributable{
    void setTimeRange(int low,long high);

    void returnAllVersions();

    void setFilter(DataFilter txnFilter);

    byte[] key();

    DataFilter filter();

    long highTimestamp();

    long lowTimestamp();
}
