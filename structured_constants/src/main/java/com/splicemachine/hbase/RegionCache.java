package com.splicemachine.hbase;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;

/**
 * @author Scott Fines
 *         Created on: 9/24/13
 */
public interface RegionCache {
    void start();

    void shutdown();

    SortedSet<HRegionInfo> getRegions(TableName tableName) throws ExecutionException;

    void invalidate(TableName tableName);

    long size();

    long getUpdateTimestamp();

    void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException;
}
