package com.splicemachine.hbase;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;

import javax.management.*;
import java.util.Collection;
import java.util.List;
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

    SortedSet<HRegionInfo> getRegionsInRange(byte[] tableName,byte[] startRow, byte[] stopRow) throws ExecutionException;
}
