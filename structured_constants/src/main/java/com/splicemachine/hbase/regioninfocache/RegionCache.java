package com.splicemachine.hbase.regioninfocache;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;
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

    SortedSet<Pair<HRegionInfo,ServerName>> getRegions(byte[] tableName) throws ExecutionException;

    void invalidate(byte[] tableName);

    long size();

    long getUpdateTimestamp();

    void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException;

    SortedSet<Pair<HRegionInfo,ServerName>> getRegionsInRange(byte[] tableName,byte[] startRow, byte[] stopRow) throws ExecutionException;
}
