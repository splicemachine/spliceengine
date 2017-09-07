/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.compactions;

import com.google.protobuf.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.quotas.RegionServerQuotaManager;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ServerNonceManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by dgomezferro on 07/09/2017.
 */
public class MockRegionServerServices implements RegionServerServices {

    public MockRegionServerServices(InetSocketAddress[] addresses) {
        this.addresses = addresses;
    }

    private InetSocketAddress[] addresses;

    @Override
    public boolean isStopping() {
        return false;
    }

    @Override
    public WAL getWAL(HRegionInfo regionInfo) throws IOException {
        return null;
    }

    @Override
    public CompactionRequestor getCompactionRequester() {
        return null;
    }

    @Override
    public FlushRequester getFlushRequester() {
        return null;
    }

    @Override
    public RegionServerAccounting getRegionServerAccounting() {
        return null;
    }

    @Override
    public TableLockManager getTableLockManager() {
        return null;
    }

    @Override
    public RegionServerQuotaManager getRegionServerQuotaManager() {
        return null;
    }

    @Override
    public void postOpenDeployTasks(PostOpenDeployContext context) throws KeeperException, IOException {

    }

    @Override
    public void postOpenDeployTasks(Region r) throws KeeperException, IOException {

    }

    @Override
    public boolean reportRegionStateTransition(RegionStateTransitionContext context) {
        return false;
    }

    @Override
    public boolean reportRegionStateTransition(RegionServerStatusProtos.RegionStateTransition.TransitionCode code, long openSeqNum, HRegionInfo... hris) {
        return false;
    }

    @Override
    public boolean reportRegionStateTransition(RegionServerStatusProtos.RegionStateTransition.TransitionCode code, HRegionInfo... hris) {
        return false;
    }

    @Override
    public RpcServerInterface getRpcServer() {
        return null;
    }

    @Override
    public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
        return null;
    }

    @Override
    public FileSystem getFileSystem() {
        return null;
    }

    @Override
    public Leases getLeases() {
        return null;
    }

    @Override
    public ExecutorService getExecutorService() {
        return null;
    }

    @Override
    public Map<String, Region> getRecoveringRegions() {
        return null;
    }

    @Override
    public ServerNonceManager getNonceManager() {
        return null;
    }

    @Override
    public boolean registerService(Service service) {
        return false;
    }

    @Override
    public HeapMemoryManager getHeapMemoryManager() {
        return null;
    }

    @Override
    public double getCompactionPressure() {
        return 0;
    }

    @Override
    public Set<TableName> getOnlineTables() {
        return null;
    }

    @Override
    public void addToOnlineRegions(Region r) {

    }

    @Override
    public boolean removeFromOnlineRegions(Region r, ServerName destination) {
        return false;
    }

    @Override
    public Region getFromOnlineRegions(String encodedRegionName) {
        return null;
    }

    @Override
    public List<Region> getOnlineRegions(TableName tableName) throws IOException {
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
        return null;
    }

    @Override
    public ClusterConnection getConnection() {
        return null;
    }

    @Override
    public MetaTableLocator getMetaTableLocator() {
        return null;
    }

    @Override
    public ServerName getServerName() {
        return null;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
        return null;
    }

    @Override
    public ChoreService getChoreService() {
        return null;
    }

    @Override
    public void abort(String why, Throwable e) {

    }

    @Override
    public boolean isAborted() {
        return false;
    }

    @Override
    public void stop(String s) {

    }

    @Override
    public boolean isStopped() {
        return false;
    }

    @Override
    public void updateRegionFavoredNodesMapping(String encodedRegionName, List<HBaseProtos.ServerName> favoredNodes) {

    }

    @Override
    public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
        return addresses;
    }
}
