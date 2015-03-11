package com.splicemachine.derby.iapi.catalog;


import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class PhysicalStatsDescriptor extends TupleDescriptor {
    private final String hostName;
    private final int numCores;
    private final long heapSize;
    private final int numIpcThreads;

    public PhysicalStatsDescriptor(String hostName,
                                   int numCores,
                                   long heapSize,
                                   int numIpcThreads){
        this.hostName = hostName;
        this.numCores = numCores;
        this.heapSize = heapSize;
        this.numIpcThreads = numIpcThreads;
    }

    public String getHostName() { return hostName; }
    public int getNumCores() { return numCores; }
    public long getHeapSize() { return heapSize; }
    public int getNumIpcThreads() { return numIpcThreads; }
}
