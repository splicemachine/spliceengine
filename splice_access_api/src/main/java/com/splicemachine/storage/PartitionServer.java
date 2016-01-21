package com.splicemachine.storage;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface PartitionServer extends Comparable<PartitionServer>{
    String getHostname();

    String getHostAndPort();

    int getPort();

    PartitionServerLoad getLoad() throws IOException;

    long getStartupTimestamp();

//    ServerLogging getLogging();
//
//    DatabaseVersion getVersion();
}
