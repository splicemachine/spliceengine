/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package org.apache.hadoop.hbase.regionserver;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.TableAuthManager;
import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBasePlatformUtils{
    private static final Logger LOG = Logger.getLogger(HBasePlatformUtils.class);

    public static void updateWriteRequests(HRegion region, long numWrites) {
		Counter writeRequestsCount = region.writeRequestsCount;
		if (writeRequestsCount != null)
			writeRequestsCount.add(numWrites);
	}

	public static void updateReadRequests(HRegion region, long numReads) {
		Counter readRequestsCount = region.readRequestsCount;
		if (readRequestsCount != null)
			readRequestsCount.add(numReads);
	}

	public static Map<byte[],Store> getStores(HRegion region) {
        List<Store> stores = region.getStores();
        HashMap<byte[],Store> storesMap = new HashMap<>();
        for (Store store: stores) {
            storesMap.put(store.getFamily().getName(),store);
        }
		return storesMap;
	}

    public static void flush(HRegion region) throws IOException {
        region.flushcache(false,false);
    }

    public static void bulkLoadHFiles(HRegion region, List<Pair<byte[], String>> copyPaths) throws IOException{
        // Is Null LISTENER Correct TODO Jun
        region.bulkLoadHFiles(copyPaths,true,null);
    }
    public static long getMemstoreSize(HRegion region) {
        return region.getMemstoreSize();
    }

    public static long getReadpoint(HRegion region) {
        return region.getMVCC().memstoreReadPoint();
    }

    public static void validateClusterKey(String quorumAddress) throws IOException {
        ZKConfig.validateClusterKey(quorumAddress);
    }

    public static boolean scannerEndReached(ScannerContext scannerContext) {
        scannerContext.setSizeLimitScope(ScannerContext.LimitScope.BETWEEN_ROWS);
        scannerContext.incrementBatchProgress(1);
        scannerContext.incrementSizeProgress(100l);
        return scannerContext.setScannerState(ScannerContext.NextState.BATCH_LIMIT_REACHED).hasMoreValues();
    }

    public static TableName getTableName(RegionCoprocessorEnvironment e) {
        return e.getRegion().getTableDesc().getTableName();
    }

    public static String getTableNameAsString(Region r) {
        return r.getTableDesc().getTableName().getQualifierAsString();
    }

    public static RpcCallContext getRpcCallContext() {
        return RpcServer.getCurrentCall();
    }

    public static HTableDescriptor getTableDescriptor(Region r) {
        return (HTableDescriptor)r.getTableDesc();
    }

    public static StoreFileInfo getFileInfo(StoreFile file) {
        return file.getFileInfo();
    }

    public static StoreFile.Reader getReader(StoreFile file) {
        return file.getReader();
    }

    public static RegionServerServices getRegionServerServices(RegionCoprocessorEnvironment e) {
        return e.getRegionServerServices();
    }

    public static RegionServerServices getRegionServerServices(RegionServerCoprocessorEnvironment e) {
        return e.getRegionServerServices();
    }

    public static User getUser() {
        return RpcServer.getRequestUser();
    }

    public static TableAuthManager getTableAuthManager(RegionCoprocessorEnvironment regionEnv) throws IOException{
        ZooKeeperWatcher zk = regionEnv.getRegionServerServices().getZooKeeper();
        return TableAuthManager.get(zk, regionEnv.getConfiguration());
    }

    public static ServerName getServerName(ObserverContext<MasterCoprocessorEnvironment> ctx) {
        return ctx.getEnvironment().getMasterServices().getServerName();
    }

    public static String getRsZNode(RegionServerServices regionServerServices) {
        return regionServerServices.getZooKeeper().rsZNode;
    }

    public static int getStorefileIndexSizeMB(ClusterStatusProtos.RegionLoad load) {
        return (int)load.getStorefileIndexSizeMB();
    }

    public static HFile.Reader createHFileReader(Configuration conf, FileSystem fs, Path path) throws IOException {
        CacheConfig cacheConf = new CacheConfig(conf);
        return HFile.createReader(fs, path, cacheConf, conf);
    }

    public static Cell getCell(HFileScanner scanner) {
        return scanner.getKeyValue();
    }

    public static StoreFile createStoreFile(FileSystem fs, StoreFileInfo info, Configuration conf, BloomType bloomType) throws IOException {
        return new StoreFile(fs, info, conf, new CacheConfig(conf),bloomType);
    }

    public static ScanInfo getScanInfo(Store store) {
        return ((HStore)store).getScanInfo();
    }

    public static StoreScanner createStoreScanner(Store store, List<StoreFileScanner> scanners, ScanType scanType,
                                                  long smallestReadPoint, long earliestPutTs) throws IOException{
        Scan scan = new Scan();
        scan.setMaxVersions(store.getFamily().getMaxVersions());
        return new StoreScanner(store, store.getScanInfo(), scan, scanners,
                scanType, smallestReadPoint, earliestPutTs);
    }

    public static void bulkLoad(Configuration conf, LoadIncrementalHFiles loader,
                                Path restorePath, String fullTableName) throws IOException {
        loader.doBulkLoad(restorePath, new HTable(conf, fullTableName));
    }

    public static HalfStoreFileReader createHalfStoreFileReader(FileSystem fs,
                                                                Path inFile,
                                                                CacheConfig cacheConf,
                                                                Reference reference,
                                                                Configuration conf) throws IOException {
        return new HalfStoreFileReader(fs, inFile, cacheConf, reference, conf);
    }

    public static ResourceTracker getResourceTracker(ResourceTrackerService rt) {
        return new ResourceTracker() {

            @Override
            public NodeHeartbeatResponse nodeHeartbeat(
                    NodeHeartbeatRequest request) throws YarnException,
                    IOException {
                NodeHeartbeatResponse response;
                try {
                    response = rt.nodeHeartbeat(request);
                } catch (YarnException e) {
                    LOG.info("Exception in heartbeat from node " +
                            request.getNodeStatus().getNodeId(), e);
                    throw e;
                }
                return response;
            }

            @Override
            public RegisterNodeManagerResponse registerNodeManager(
                    RegisterNodeManagerRequest request)
                    throws YarnException, IOException {
                RegisterNodeManagerResponse response;
                try {
                    response = rt.registerNodeManager(request);
                } catch (YarnException e) {
                    LOG.info("Exception in node registration from "
                            + request.getNodeId().toString(), e);
                    throw e;
                }
                return response;
            }
        };
    }
}
