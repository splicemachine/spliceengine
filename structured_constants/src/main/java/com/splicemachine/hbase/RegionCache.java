package com.splicemachine.hbase;

import org.apache.hadoop.hbase.HRegionInfo;

import javax.management.*;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 9/24/13
 */
public interface RegionCache {
    void start();

    void shutdown();

    SortedSet<HRegionInfo> getRegions(byte[] tableName) throws ExecutionException;

    void invalidate(byte[] tableName);

    long size();

    long getUpdateTimestamp();

    void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException;
}
