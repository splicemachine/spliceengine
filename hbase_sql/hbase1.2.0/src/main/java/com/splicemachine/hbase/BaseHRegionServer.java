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

package com.splicemachine.hbase;

import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Created by jyuan on 2/12/19.
 */
public class BaseHRegionServer implements AdminProtos.AdminService.BlockingInterface, Server, PriorityFunction {
    public AdminProtos.GetRegionInfoResponse getRegionInfo(RpcController rpcController, AdminProtos.GetRegionInfoRequest getRegionInfoRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.GetStoreFileResponse getStoreFile(RpcController rpcController, AdminProtos.GetStoreFileRequest getStoreFileRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.GetOnlineRegionResponse getOnlineRegion(RpcController rpcController, AdminProtos.GetOnlineRegionRequest getOnlineRegionRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.OpenRegionResponse openRegion(RpcController rpcController, AdminProtos.OpenRegionRequest openRegionRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.CloseRegionResponse closeRegion(RpcController rpcController, AdminProtos.CloseRegionRequest closeRegionRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller, AdminProtos.UpdateFavoredNodesRequest request) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.FlushRegionResponse flushRegion(RpcController rpcController, AdminProtos.FlushRegionRequest flushRegionRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.SplitRegionResponse splitRegion(RpcController rpcController, AdminProtos.SplitRegionRequest splitRegionRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.CompactRegionResponse compactRegion(RpcController rpcController, AdminProtos.CompactRegionRequest compactRegionRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.MergeRegionsResponse mergeRegions(RpcController rpcController, AdminProtos.MergeRegionsRequest mergeRegionsRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(RpcController rpcController, AdminProtos.ReplicateWALEntryRequest replicateWALEntryRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.ReplicateWALEntryResponse replay(RpcController controller, AdminProtos.ReplicateWALEntryRequest request) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.RollWALWriterResponse rollWALWriter(RpcController rpcController, AdminProtos.RollWALWriterRequest rollWALWriterRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.GetServerInfoResponse getServerInfo(RpcController rpcController, AdminProtos.GetServerInfoRequest getServerInfoRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.StopServerResponse stopServer(RpcController rpcController, AdminProtos.StopServerRequest stopServerRequest) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Configuration getConfiguration() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public ZooKeeperWatcher getZooKeeper() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public ServerName getServerName() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void abort(String s, Throwable throwable) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public boolean isAborted() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void stop(String s) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public boolean isStopped() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public int getPriority(RPCProtos.RequestHeader header, Message param, User user) {
        return 0;
    }

    public int getPriority(RPCProtos.RequestHeader header, Message param){return 0;}

    public long getDeadline(RPCProtos.RequestHeader header, Message param) {
        return 0L;
    }

    public ClusterConnection getConnection() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public MetaTableLocator getMetaTableLocator() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public CoordinatedStateManager getCoordinatedStateManager() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public ChoreService getChoreService() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.UpdateConfigurationResponse updateConfiguration(RpcController controller, AdminProtos.UpdateConfigurationRequest request) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public AdminProtos.WarmupRegionResponse warmupRegion(RpcController controller, AdminProtos.WarmupRegionRequest request) throws ServiceException {
        throw new UnsupportedOperationException("Not implemented");
    }
}
