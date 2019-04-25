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


import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.TableAuthManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.*;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class HBasePlatformUtils{
    private static final Logger LOG = Logger.getLogger(HBasePlatformUtils.class);

	public static void updateWriteRequests(HRegion region, long numWrites) {
		LongAdder writeRequestsCount = region.writeRequestsCount;
		if (writeRequestsCount != null)
			writeRequestsCount.add(numWrites);
	}

	public static void updateReadRequests(HRegion region, long numReads) {
		LongAdder readRequestsCount = region.readRequestsCount;
		if (readRequestsCount != null)
			readRequestsCount.add(numReads);
	}

	public static Map<byte[],Store> getStores(HRegion region) {
        List<HStore> stores = region.getStores();
        HashMap<byte[],Store> storesMap = new HashMap<>();
        for (Store store: stores) {
            storesMap.put(store.getColumnFamilyDescriptor().getName(),store);
        }
		return storesMap;
	}

    public static void flush(HRegion region) throws IOException {
        region.flushcache(false,false, null);
    }

    public static void bulkLoadHFiles(HRegion region, List<Pair<byte[], String>> copyPaths) throws IOException{
        // Is Null LISTENER Correct TODO Jun
        region.bulkLoadHFiles(copyPaths,true,null);
    }
    public static long getMemstoreSize(HRegion region) {
        return region.getMemStoreHeapSize();
    }


    public static long getReadpoint(HRegion region) {
        return region.getMVCC().getReadPoint();
    }


    public static void validateClusterKey(String quorumAddress) throws IOException {
        ZKConfig.validateClusterKey(quorumAddress);
    }

    public static boolean scannerEndReached(ScannerContext scannerContext) {
        scannerContext.setSizeLimitScope(ScannerContext.LimitScope.BETWEEN_ROWS);
        scannerContext.incrementBatchProgress(1);
        scannerContext.incrementSizeProgress(100l, 100l);
        return scannerContext.setScannerState(ScannerContext.NextState.BATCH_LIMIT_REACHED).hasMoreValues();
    }

    public static TableName getTableName(RegionCoprocessorEnvironment e) {
        return e.getRegion().getTableDescriptor().getTableName();
    }

    public static RpcCallContext getRpcCallContext() {
        Optional<RpcCall> rpcCall = RpcServer.getCurrentCall();
        return rpcCall.isPresent() ? rpcCall.get() : null;
    }

    public static HTableDescriptor getTableDescriptor(Region r) {
        return (HTableDescriptor)r.getTableDescriptor();
    }

    public static String getTableNameAsString(Region r) {
        return r.getTableDescriptor().getTableName().getQualifierAsString();
    }
    public static StoreFileInfo getFileInfo(StoreFile file) {
        return ((HStoreFile)file).getFileInfo();
    }

    public static StoreFileReader getReader(StoreFile file) {
        return ((HStoreFile)file).getReader();
    }

    public static RegionServerServices getRegionServerServices(RegionCoprocessorEnvironment e) {
        return (RegionServerServices)e.getOnlineRegions();
    }

    public static RegionServerServices getRegionServerServices(RegionServerCoprocessorEnvironment e) {
        return (RegionServerServices)e.getOnlineRegions();
    }

    public static User getUser() {
        return RpcServer.getRequestUser().get();
    }

    public static TableAuthManager getTableAuthManager(RegionCoprocessorEnvironment regionEnv) throws IOException{
        ZKWatcher zk = ((RegionServerServices)regionEnv.getOnlineRegions()).getZooKeeper();
        return TableAuthManager.getOrCreate(zk, regionEnv.getConfiguration());
    }

    public static ServerName getServerName(ObserverContext<MasterCoprocessorEnvironment> ctx) {
        return ctx.getEnvironment().getServerName();
    }

    public static String getRsZNode(RegionServerServices regionServerServices) {
        return regionServerServices.getZooKeeper().getZNodePaths().rsZNode;
    }

    public static int getStorefileIndexSizeMB(ClusterStatusProtos.RegionLoad load) {
        return (int)load.getStorefileIndexSizeKB()/1024;
    }

    public static HFile.Reader createHFileReader(Configuration conf, FileSystem fs, Path path) throws IOException {
        return HFile.createReader(fs, path, conf);
    }

    public static Cell getCell(HFileScanner scanner) {
        return scanner.getCell();
    }

    public static StoreFile createStoreFile(FileSystem fs, StoreFileInfo info, Configuration conf, BloomType bloomType) {
        return new HStoreFile(fs, info, conf, new CacheConfig(conf), bloomType, true);
    }

    public static ScanInfo getScanInfo(Store store) {
        return ((HStore)store).getScanInfo();
    }

    public static StoreScanner createStoreScanner(Store store, List<StoreFileScanner> scanners, ScanType scanType,
                                                  long smallestReadPoint, long earliestPutTs) throws IOException{

        return new StoreScanner((HStore)store, ((HStore)store).getScanInfo(), scanners, scanType,
                smallestReadPoint, earliestPutTs);
    }

    public static void bulkLoad(Configuration conf, LoadIncrementalHFiles loader,
                                Path path, String fullTableName) throws IOException {
        SConfiguration configuration = HConfiguration.getConfiguration();
        org.apache.hadoop.hbase.client.Connection conn = HBaseConnectionFactory.getInstance(configuration).getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        TableName tableName = TableName.valueOf(fullTableName);
        RegionLocator locator = conn.getRegionLocator(tableName);
        Table table = conn.getTable(tableName);
        loader.doBulkLoad(path, admin, table, locator);
    }

    public static HalfStoreFileReader createHalfStoreFileReader(FileSystem fs,
                                                                Path inFile,
                                                                CacheConfig cacheConf,
                                                                Reference reference,
                                                                Configuration conf) throws IOException {
        return new HalfStoreFileReader(fs, inFile, cacheConf, reference, true, new AtomicInteger(0), true, conf);
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

                } catch (NullPointerException npe) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ie) {
                        throw new IOException(ie);
                    }
                    return registerNodeManager(request);
                } catch (YarnException e) {
                    LOG.info("Exception in node registration from "
                            + request.getNodeId().toString(), e);
                    throw e;
                }
                return response;
            }

            @Override
            public UnRegisterNodeManagerResponse unRegisterNodeManager(
                    UnRegisterNodeManagerRequest request) throws YarnException, IOException {
                UnRegisterNodeManagerResponse response;
                try {
                    response = rt.unRegisterNodeManager(request);
                } catch (YarnException e) {
                    LOG.info("Exception in node registration from "
                            + request.getNodeId().toString(), e);
                    throw e;
                }
                return response;
            }
        };
    }

    public static void waitForNMToRegister(NodeManager nm) throws Exception{
        NMTokenSecretManagerInNM nmTokenSecretManagerNM =
                nm.getNMContext().getNMTokenSecretManager();
        NMContainerTokenSecretManager containerTokenSecretManager = nm.getNMContext().getContainerTokenSecretManager();
        int attempt = 60;
        while(attempt-- > 0) {
            try {
                if (nmTokenSecretManagerNM.getCurrentKey() != null && containerTokenSecretManager.getCurrentKey() != null) {
                    break;
                }
            } catch (Exception e) {

            }
            Thread.sleep(2000);
        }
    }
}
