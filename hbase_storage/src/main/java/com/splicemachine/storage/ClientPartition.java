/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.storage;

import splice.com.google.common.base.Function;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.impl.HNotServingRegion;
import com.splicemachine.si.impl.HRegionTooBusy;
import com.splicemachine.si.impl.HWrongRegion;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.ImmutableList;
import splice.com.google.common.collect.Iterables;
import splice.com.google.common.collect.Iterators;
import com.google.protobuf.Service;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.util.PartitionInRangePredicate;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Represents an HBase Table as a single Partition.
 *
 * @author Scott Fines
 *         Date: 12/17/15
 */
@NotThreadSafe
public class ClientPartition extends SkeletonHBaseClientPartition{
    protected static final Logger LOG = Logger.getLogger(ClientPartition.class);
    private TableName tableName;
    private final Table table;
    private final Connection connection;
    private final Clock clock;
    private PartitionInfoCache<TableName> partitionInfoCache;

    public ClientPartition(Connection connection,
                           TableName tableName,
                           Table table,
                           Clock clock,
                           PartitionInfoCache<TableName> partitionInfoCache){
        assert tableName!=null:"Passed in tableName is null";
        this.tableName=tableName;
        this.table=table;
        this.connection=connection;
        this.clock = clock;
        this.partitionInfoCache = partitionInfoCache;
    }

    @Override
    public String getTableName(){
        return table.getName().getQualifierAsString();
    }

    @Override
    public void close() throws IOException{
        table.close();
    }

    @Override
    protected Result doGet(Get get) throws IOException{
        return table.get(get);
    }

    @Override
    protected ResultScanner getScanner(Scan scan) throws IOException{
        return table.getScanner(scan);
    }

    @Override
    protected void doDelete(Delete delete) throws IOException{
        table.delete(delete);
    }

    @Override
    protected void doDelete(List<Delete> delete) throws IOException{
        table.delete(delete);
    }

    @Override
    protected void doPut(Put put) throws IOException{
        if (LOG.isDebugEnabled()) {
            logPut(put);
        }
        table.put(put);
    }

    private void logPut(Put put) {
        if (put.has(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.TOMBSTONE_COLUMN_BYTES)) {
            LOG.debug("Writing tombstone " + put + " to table " + table.getName());
        } else {
            LOG.debug("Writing entry " + put + " to table " + table.getName());
        }
    }

    @Override
    protected void doPut(List<Put> puts) throws IOException{
        if (LOG.isDebugEnabled()) {
            for (Put put : puts) {
                logPut(put);
            }
        }
        table.put(puts);
    }

    @Override
    protected long doIncrement(Increment incr) throws IOException{
        return Bytes.toLong(table.increment(incr).value());
    }

    @Override
    public Iterator<DataResult> batchGet(Attributable attributes,List<byte[]> rowKeys) throws IOException{
        List<Get> gets=new ArrayList<>(rowKeys.size());
        for(byte[] rowKey : rowKeys){
            Get g=new Get(rowKey);
            if(attributes!=null){
                for(Map.Entry<String, byte[]> attr : attributes.allAttributes().entrySet()){
                    g.setAttribute(attr.getKey(),attr.getValue());
                }
            }
            gets.add(g);
        }
        Result[] results=table.get(gets);
        if(results.length<=0) return Collections.emptyIterator();
        final HResult retResult=new HResult();
        return Iterators.transform(Iterators.forArray(results),new Function<Result, DataResult>(){
            @Override
            public DataResult apply(Result input){
                retResult.set(input);
                return retResult;
            }
        });
    }

    @Override
    public boolean checkAndPut(byte[] key,byte[] family,byte[] qualifier,byte[] expectedValue,DataPut put) throws IOException{
        return table.checkAndPut(key,family,qualifier,expectedValue,((HPut)put).unwrapDelegate());
    }

    @Override
    public List<Partition> subPartitions(){
        return subPartitions(false);
    }

    public List<Partition> subPartitions(boolean refresh) {
        try {
            List<Partition> partitions;
            if (!refresh) {
                partitions = partitionInfoCache.getIfPresent(tableName);
                if (partitions != null) {
                    return partitions;
                }
            }
            List<HRegionLocation> tableLocations = ((ClusterConnection) connection).locateRegions(tableName, !refresh, false);
            if (outdated(tableLocations)) {
                tableLocations = ((ClusterConnection) connection).locateRegions(tableName, false, false);
            }
            partitions = new ArrayList<>(tableLocations.size());
            for (HRegionLocation location : tableLocations){
                if (location.getServerName() == null) {     // ensure no offline regions
                    continue;
                }
                HRegionInfo regionInfo = location.getRegionInfo();
                partitions.add(new RangedClientPartition(this, regionInfo, new RLServer(location)));
            }
            partitionInfoCache.put(tableName, partitions);
            return partitions;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // The connection tried to locate a region busing its start key. If the cache is being used(by default) and contains
    // stale entries, the returned region locations contains duplicate entries. The duplicate entry is a region that has
    // be split.
    private boolean outdated(List<HRegionLocation> tableLocations) {
        Set<String> regionLocations = new HashSet<>();
        for (HRegionLocation regionLocation : tableLocations) {
            String encodedName = regionLocation.getRegion().getEncodedName();
            if (regionLocations.contains(encodedName))
                return true;
            else
                regionLocations.add(encodedName);
        }
        return false;
    }

    @Override
    public PartitionServer owningServer(){
        throw new UnsupportedOperationException("A Table is not owned by a single server, but by the cluster as a whole");
    }
    @Override
    public List<Partition> subPartitions(byte[] startRow,byte[] stopRow) {
        return subPartitions(startRow,stopRow,false);
    }

    @Override
    public List<Partition> subPartitions(byte[] startRow,byte[] stopRow, boolean refresh) {
        return ImmutableList.copyOf(Iterables.filter(subPartitions(refresh),new PartitionInRangePredicate(startRow,stopRow)));
    }

    @Override
    public PartitionLoad getLoad() throws IOException {
        Map<ServerName, List<RegionInfo>> serverToRegionMap = new HashMap<>();
        try (RegionLocator rl = connection.getRegionLocator(tableName)) {
            List<HRegionLocation> locations = rl.getAllRegionLocations();
            for (HRegionLocation location : locations) {
                List<RegionInfo> info = serverToRegionMap.get(location.getServerName());
                if (info == null) {
                    info = new LinkedList<>();
                    serverToRegionMap.put(location.getServerName(), info);
                }
                info.add(location.getRegion());
            }
        }
        long totalStoreFileSize = 0;
        long totalMemstoreSize = 0;
        try (Admin admin = connection.getAdmin()) {
            ClusterMetrics metrics = admin.getClusterMetrics();
            for (Map.Entry<ServerName, List<RegionInfo>> entry : serverToRegionMap.entrySet()) {
                ServerName serverName = entry.getKey();
                ServerMetrics serverMetrics = metrics.getLiveServerMetrics().get(serverName);
                Map<byte[], RegionMetrics> regionsMetrics = serverMetrics.getRegionMetrics();
                for (RegionInfo regionInfo : entry.getValue()) {
                    RegionMetrics regionMetrics = regionsMetrics.get(regionInfo.getRegionName());
                    if (regionMetrics != null) {
                        totalStoreFileSize += regionMetrics.getStoreFileSize().get(Size.Unit.BYTE);
                        totalMemstoreSize += regionMetrics.getMemStoreSize().get(Size.Unit.BYTE);
                    }
                }
            }
        }
        return new HPartitionLoad(getName(), totalStoreFileSize, totalMemstoreSize);
    }

    /**
     * Major compacts the table. Synchronous operation.
     * @throws java.io.IOException
     */
    @Override
    public void compact(boolean isMajor) throws IOException{
        HExceptionFactory exceptionFactory = HExceptionFactory.INSTANCE;
        try(Admin admin = connection.getAdmin()){
            if (isMajor)
                admin.majorCompact(tableName);
            else
                admin.compact(tableName);
            CompactionState compactionState=null;
            int retriesForNPE = 0;
            do{
                try{
                    clock.sleep(500l,TimeUnit.MILLISECONDS);
                }catch(InterruptedException e){
                    throw new InterruptedIOException();
                }

                try {
                    compactionState = admin.getCompactionState(tableName);
                } catch (NullPointerException e) {
                    if (LOG.isDebugEnabled()) {
                        SpliceLogUtils.debug(LOG,
                                "Failed to fetch compaction state with NPE for table %s but we will keep trying: %s.",
                                tableName.getQualifierAsString(), e);
                    }

                    retriesForNPE ++;
                    if (retriesForNPE >= 100) {
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG,
                                    "Failed to fetch compaction state with NPE for table %s for >=100 times, give up!",
                                    tableName.getQualifierAsString());
                        }
                        throw e;
                    }
                } catch(Exception e){
                    // Catch and ignore the typical region errors while checking compaction state.
                    // Otherwise the client compaction request will fail which we don't want.
                    IOException ioe = exceptionFactory.processRemoteException(e);
                    if (ioe instanceof HNotServingRegion ||
                        ioe instanceof HWrongRegion ||
                        ioe instanceof HRegionTooBusy) {
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG,
                                "Can not fetch compaction state for table %s but we will keep trying: %s.",
                                tableName.getQualifierAsString(), ioe);
                        }
                    } else {
                        throw e;
                    }
                }
            }while(compactionState!=CompactionState.NONE);
        }
    }
    /**
     * Flush a table. Synchronous operation.
     * @throws java.io.IOException
     */
    @Override
    public void flush() throws IOException {
        try(Admin admin = connection.getAdmin()) {
            admin.flush(tableName);
        }
    }

    public <T extends Service,V> Map<byte[],V> coprocessorExec(Class<T> serviceClass,Batch.Call<T,V> call) throws Throwable{
        return table.coprocessorService(serviceClass,getStartKey(),getEndKey(),call);
    }

    public <T extends Service,V> Map<byte[],V> coprocessorExec(
            Class<T> serviceClass,
            byte[] startKey,
            byte[] endKey,
            Batch.Call<T,V> call) throws Throwable{
        return table.coprocessorService(serviceClass, startKey, endKey, call);
    }

    public Table unwrapDelegate(){
        return table;
    }

    @Override
    public BitSet getBloomInMemoryCheck(boolean hasConstraintChecker,Pair<KVPair, Lock>[] dataAndLocks) throws IOException {
        return null;
    }

    @Override
    public PartitionDescriptor getDescriptor() throws IOException {
        return new HPartitionDescriptor(table.getDescriptor());
    }
}
