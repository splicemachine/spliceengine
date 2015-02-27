package com.splicemachine.derby.iapi.catalog;

import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class PhysicalStatsDescriptor extends TupleDescriptor {
    private final String hostName;
    private final int numCores;
    private final long heapSize;
    private final int numIpcThreads;
    private final long localReadLatency;
    private final long remoteReadLatency;
    private final long writeLatency;

    public PhysicalStatsDescriptor(String hostName,
                                   int numCores,
                                   long heapSize,
                                   int numIpcThreads,
                                   long localReadLatency,
                                   long remoteReadLatency,
                                   long writeLatency) {
        this.hostName = hostName;
        this.numCores = numCores;
        this.heapSize = heapSize;
        this.numIpcThreads = numIpcThreads;
        this.localReadLatency = localReadLatency;
        this.remoteReadLatency = remoteReadLatency;
        this.writeLatency = writeLatency;
    }

    public String getHostName() { return hostName; }
    public int getNumCores() { return numCores; }
    public long getHeapSize() { return heapSize; }
    public int getNumIpcThreads() { return numIpcThreads; }
    public long getLocalReadLatency() { return localReadLatency; }
    public long getRemoteReadLatency() { return remoteReadLatency; }
    public long getWriteLatency() { return writeLatency; }
}
