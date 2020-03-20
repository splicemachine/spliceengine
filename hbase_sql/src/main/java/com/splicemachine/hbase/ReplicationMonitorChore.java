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

package com.splicemachine.hbase;

import com.clearspring.analytics.util.Lists;
import com.jcraft.jsch.JSchException;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.replication.ReplicationSystemProcedure;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jyuan on 9/30/19.
 */
public class ReplicationMonitorChore extends ScheduledChore {
    private static final Logger LOG = Logger.getLogger(ReplicationMonitorChore.class);

    // cluster key for active cluster. A cluster key contains zookeeper quorum and hbase znode path
    private String masterCluster;
    private String thisCluster;
    private String replicationMonitorQuorum;
    private String replicationMonitorPath;
    private volatile boolean isLeader;
    private boolean isMaster;
    private boolean isChoreService;
    private String user;
    private String password;
    private List<String> peers;

    private ZKWatcher zkWatcher;
    RecoverableZooKeeper rzk;
    private Configuration conf;
    private List<String> regionServers;
    public static final String DB_URL_LOCAL = "jdbc:splice://%s/splicedb;user=splice;password=admin";

    private Map<String, Connection> connectionPool = new HashMap<>();

    private String healthcheckScript;

    // This constructor is for chore services
    public ReplicationMonitorChore(final String name,
                                   final Stoppable stopper,
                                   final int period) throws SQLException, IOException, KeeperException, InterruptedException {
        super(name, stopper, period);
        this.replicationMonitorQuorum = HConfiguration.getConfiguration().getReplicationMonitorQuorum();
        this.replicationMonitorPath = HConfiguration.getConfiguration().getReplicationMonitorPath();
        this.thisCluster = ReplicationSystemProcedure.getClusterKey().replaceAll("/","");
        isChoreService = true;
        init();
    }


    // This constructor is for standalone monitor
    public ReplicationMonitorChore(String replicationMonitorQuorum,
                                   String replicationMonitorPath,
                                   String thisCluster) throws SQLException, IOException, KeeperException, InterruptedException {
        this.replicationMonitorQuorum = replicationMonitorQuorum;
        this.replicationMonitorPath = replicationMonitorPath;
        this.thisCluster = thisCluster;
        init();
    }

    // This constructor is for performing failover.
    public ReplicationMonitorChore(String replicationMonitorQuorum,
                                   String replicationMonitorPath,
                                   String user,
                                   String password) throws SQLException,
            IOException, KeeperException, InterruptedException {

        this.replicationMonitorQuorum = replicationMonitorQuorum;
        this.replicationMonitorPath = replicationMonitorPath;
        this.user = user;
        this.password = password;
        init();
    }

    private void init() throws SQLException, IOException, KeeperException, InterruptedException {
        conf = ReplicationUtils.createConfiguration(replicationMonitorQuorum);
        zkWatcher = new ZKWatcher(conf, "replication monitor", null, false);
        rzk = zkWatcher.getRecoverableZooKeeper();
        if (thisCluster != null) {
            String path = replicationMonitorPath + "/monitors/" + thisCluster;
            ZkUtils.recursiveSafeCreate(path, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, rzk);
            determineLeadership();
        }
    }

    public void determineLeadership() throws KeeperException, InterruptedException{

        String path = replicationMonitorPath+"/monitors";
        List<String> children = rzk.getChildren(path,
                new ReplicationMonitorWatcher(this, rzk, path, thisCluster));
        Collections.sort(children);
        if (thisCluster != null) {
            isLeader = thisCluster.equals(children.get(0));
        }
        if (isLeader) {
            SpliceLogUtils.info(LOG, "%s becomes leading monitor", thisCluster);
        }
    }

    private List<String> getRegionServers() throws IOException, InterruptedException, KeeperException {

        String masterClusterKey = getClusterKey(masterCluster);
        Configuration conf = ReplicationUtils.createConfiguration(masterClusterKey);
        return ReplicationUtils.getRegionServers(conf);
    }

    /**
     * establish a connection to master cluster
     * @throws SQLException
     */
    private Connection connect(String hostAndPort) throws SQLException{
        if (connectionPool.containsKey(hostAndPort)) {
            Connection connection = connectionPool.get(hostAndPort);
            if (!connection.isClosed()) {
                return connection;
            }
        }
        String url = String.format(DB_URL_LOCAL, hostAndPort);
        Connection connection = ReplicationUtils.connect(url);
        connectionPool.put(hostAndPort, connection);
        return connection;

    }

    private void healthCheckMasterCluster() throws SQLException, KeeperException, InterruptedException {
        String sql = "select count(*) from SYSIBM.SYSDUMMY1";
        int healthyCount = 0;
        for (String regionServer : regionServers) {
            try (Connection connection = connect(regionServer);
                 ResultSet rs = connection.createStatement().executeQuery(sql)) {
                if(rs.next()){
                    healthyCount++;
                }
            }
            catch (SQLException e) {
                LOG.warn("Region server " + regionServer + " may be down!");
            }
        }

        byte[] status = healthyCount >= regionServers.size() * 0.5 ? ReplicationUtils.MASTER_CLUSTER_STATUS_UP :
                ReplicationUtils.MASTER_CLUSTER_STATUS_DOWN;

        String path = replicationMonitorPath + "/monitors/" + thisCluster;
        if (rzk.exists(path, false) != null) {
            rzk.setData(path, status, -1);
        }
    }

    private String getMasterCluster() throws InterruptedException, KeeperException{
        RecoverableZooKeeper rzk = zkWatcher.getRecoverableZooKeeper();
        String path = replicationMonitorPath + "/master";
        if (rzk.exists(path, false) != null) {
            List<String> children = rzk.getChildren(path, false);
            return children.size() > 0 ? children.get(0) : null;
        }
        return null;
    }

    private boolean involvedInReplication() throws InterruptedException, KeeperException{
        RecoverableZooKeeper rzk = zkWatcher.getRecoverableZooKeeper();
        String masterPath = replicationMonitorPath + "/master/" + thisCluster;
        String peerPath = replicationMonitorPath + "/peers/" + thisCluster;
        return rzk.exists(masterPath, false) != null || rzk.exists(peerPath, false) != null;

    }

    @Override
    protected void chore(){

        boolean failover = false;

        try {
            if (isChoreService) {
                boolean replicationEnabled = HConfiguration.getConfiguration().replicationEnabled();
                if (!replicationEnabled)
                    return;
            }

            String mc = getMasterCluster();
            if (masterCluster != null && masterCluster !=mc) {
                // master cluster has changed, close all connections from the pool
                for (Map.Entry<String, Connection> entry : connectionPool.entrySet()) {
                    Connection connection = entry.getValue();
                    if (!connection.isClosed()) {
                        connection.close();
                    }
                }
            }

            if (masterCluster == null || !masterCluster.equals(mc)) {
                masterCluster = mc;
            }
            if (masterCluster != null) {
                // Get a list of region servers to test healthness of master cluster
                isMaster = masterCluster.equals(thisCluster);
                try {
                    regionServers = getRegionServers();
                    failover = (regionServers.size() == 0); // failover if no region server is up
                } catch (Exception e) {
                    SpliceLogUtils.error(LOG, "Encountered an error when trying to get a list of all region servers", e);
                    failover = true;
                }
            }
            else {
                isMaster = false;
                if (regionServers != null && regionServers.size() > 0) {
                    regionServers = Lists.newArrayList();
                }
                // No master cluster, so nothing to monitor
                return;
            }


            if (!failover) {
                healthcheckScript = SIDriver.driver().getConfiguration().getReplicationHealthcheckScript();
                if (healthcheckScript != null) {
                    runHealthcheckScript();
                }
                else {
                    healthCheckMasterCluster();
                }
            }

            // If this is leader, determine whether a failover should be triggered
            if (isLeader) {
                failover = failover || shouldFailOver();
                if (failover) {
                    sendAlert();
                }
            }
        }
        catch (Exception e) {
            LOG.info("Encountered an error:", e);
        }
    }

    private void runHealthcheckScript() throws IOException, InterruptedException, KeeperException {

        String command = healthcheckScript;
        for (String rs : regionServers) {
            String[] s = rs.split(":");
             command += " " + s[0];
        }
        String result = executeScript(command);
        byte[] status = result.equals("SUCCESS") ? ReplicationUtils.MASTER_CLUSTER_STATUS_UP :
                ReplicationUtils.MASTER_CLUSTER_STATUS_DOWN;

        String path = replicationMonitorPath + "/monitors/" + thisCluster;
        if (rzk.exists(path, false) != null) {
            rzk.setData(path, status, -1);
        }
    }

    private boolean shouldFailOver() throws KeeperException, InterruptedException{

        int total = 0;
        int count = 0;

        List<String> children = rzk.getChildren(replicationMonitorPath + "/monitors", false);
        total += children.size();
        for (String cluster:children) {
            String path = replicationMonitorPath + "/monitors/" + cluster;
            byte[] status = rzk.getData(path, false, null);
            if (Bytes.compareTo(status, ReplicationUtils.MASTER_CLUSTER_STATUS_UP) == 0) {
                count++;
            }
        }

        boolean failOver = count < total/2;
        if (failOver) {
            SpliceLogUtils.warn(LOG, "%d out of %d monitors reported master cluster is unhealthy!!!", total-count, total);
        }
        return failOver;
    }

    private void sendAlert() {

        SpliceLogUtils.warn(LOG, "The current master cluster %s is likely to be down!!! " +
                "Please run failover script !!!", masterCluster );
    }

    private static class ReplicationMonitorWatcher implements Watcher {

        private final RecoverableZooKeeper rzk;
        private final String monitorPath;
        private final String clusterKey;
        private final ReplicationMonitorChore replicationMonitor;

        public ReplicationMonitorWatcher(ReplicationMonitorChore replicationMonitor,
                                         RecoverableZooKeeper rzk,
                                         String monitorPath,
                                         String clusterKey) {
            this.replicationMonitor = replicationMonitor;
            this.rzk = rzk;
            this.monitorPath = monitorPath;
            this.clusterKey = clusterKey;
        }

        @Override
        public void process(WatchedEvent event) {
            Event.EventType type = event.getType();
            try {
                if (type == Event.EventType.NodeChildrenChanged) {
                    replicationMonitor.determineLeadership();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void performFailover() throws IOException, InterruptedException,
            KeeperException, SQLException, JSchException, ReplicationException{

        // Elect a new master. By default, choose the cluster that is most up-to-date with master
        String newMaster = electNewMaster();

        boolean masterReachable = isMasterReachable();

        long masterTimestamp = -1;
        if (masterReachable) {
            // Sync old master with new master
            syncUpWALs();

            // disable replication from old master. If it is recovered, it's no longer replicating data to other clusters
            disableMaster();

            masterTimestamp = getMasterTimestamp();
        }

        // setup replication for new master. Replication peers are not enabled until they syn up with old masters
        configureNewMaster(newMaster, masterTimestamp);
    }

    private long getMasterTimestamp() throws IOException, KeeperException, InterruptedException {
        String masterClusterKey = getClusterKey(masterCluster);
        Configuration conf = ReplicationUtils.createConfiguration(masterClusterKey);

        try (ZKWatcher masterZkw = new ZKWatcher(conf, "replication monitor", null, false)) {
            RecoverableZooKeeper rzk = masterZkw.getRecoverableZooKeeper();
            String rootNode = HConfiguration.getConfiguration().getSpliceRootPath();
            String node = rootNode + HConfiguration.MAX_RESERVED_TIMESTAMP_PATH;
            byte[] data = rzk.getData(node, false, null);
            long ts = Bytes.toLong(data);
            return ts;
        }
    }

    private boolean isMasterReachable() {
        String masterClusterKey = getClusterKey(masterCluster);
        Configuration conf = ReplicationUtils.createConfiguration(masterClusterKey);

        try (ZKWatcher masterZkw = new ZKWatcher(conf, "replication monitor", null, false)) {
            String[] s = masterClusterKey.split(":");
            RecoverableZooKeeper rzk = masterZkw.getRecoverableZooKeeper();
            List<String> children = rzk.getChildren(s[2], false);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private void syncUpWALs() throws JSchException, IOException{
        String[] s = masterCluster.split(":");
        RemoteExec sync = new RemoteExec(s[0], user, password,
                "hbase org.apache.hadoop.hbase.replication.regionserver.ReplicationSyncUp");
        String output = sync.execute();
        LOG.info(output);
    }

    private void disableMaster() throws InterruptedException, KeeperException, IOException {

        // Delete all peers from master cluster
        String masterClusterKey = getClusterKey(masterCluster);
        Configuration conf = ReplicationUtils.createConfiguration(masterClusterKey);
        ZKWatcher masterZkw = new ZKWatcher(conf, "replication monitor", null, false);
        RecoverableZooKeeper masterRzk = masterZkw.getRecoverableZooKeeper();
        String[] s = masterClusterKey.split(":");
        String hbaseRootDir = s[2];
        String peerPath = hbaseRootDir+"/replication/peers";
        List<String> peers = masterRzk.getChildren(peerPath, false);
        for (String peer : peers) {
            String p = peerPath + "/" + peer;
            List<String> children = masterRzk.getChildren(p, false);
            String peerStatePath = p + "/" + children.get(0);
            masterRzk.setData(peerStatePath, toByteArray(ReplicationProtos.ReplicationState.State.DISABLED), -1);
        }

        // Delete configuration from monitor quorum
        List<String> children = this.rzk.getChildren(replicationMonitorPath + "/master", false);
        for (String node : children) {
            this.rzk.delete(replicationMonitorPath + "/master/" + node, -1);
        }
        children = this.rzk.getChildren(replicationMonitorPath + "/peers", false);
        for (String node : children) {
            this.rzk.delete(replicationMonitorPath + "/peers/" + node, -1);
        }
    }

    protected static byte[] toByteArray(final ReplicationProtos.ReplicationState.State state) {
        ReplicationProtos.ReplicationState msg =
                ReplicationProtos.ReplicationState.newBuilder().setState(state).build();
        // There is no toByteArray on this pb Message?
        // 32 bytes is default which seems fair enough here.
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            CodedOutputStream cos = CodedOutputStream.newInstance(baos, 16);
            msg.writeTo(cos);
            cos.flush();
            baos.flush();
            return ProtobufUtil.prependPBMagic(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void configureNewMaster(String newMaster, long ts) throws IOException, InterruptedException, KeeperException, SQLException{

        Configuration conf = null;
        for (String peer : peers) {
            if (peer.equals(newMaster)) {
                String clusterKey = getClusterKey(peer);
                conf = ReplicationUtils.createConfiguration(clusterKey);
                break;
            }
        }

        String newMasterClusterKey = getClusterKey(newMaster);
        try (Connection connection = ReplicationUtils.connect(conf, newMasterClusterKey)){
            ReplicationUtils.setupReplicationMaster(connection, newMasterClusterKey);
            Integer peerId = 1;
            for (String peer : peers) {
                if (peer.equals(newMaster)) {
                    continue;
                }
                String peerClusterKey = getClusterKey(peer);
                ReplicationUtils.addPeer(connection, peerClusterKey, peerId.toString(), true, false);
                peerId++;
            }
            // Add old master as a disabled peer
            String peerClusterKey = getClusterKey(masterCluster);
            ReplicationUtils.addPeer(connection, peerClusterKey, peerId.toString(), false, false);
            if (ts > 0) {
                setNewMasterTimestamp(newMasterClusterKey, ts);
            }
        }

    }

    private void setNewMasterTimestamp(String newMasterClusterKey,
                                       long ts) throws IOException, KeeperException, InterruptedException{
        Configuration conf = ReplicationUtils.createConfiguration(newMasterClusterKey);

        try (ZKWatcher masterZkw = new ZKWatcher(conf, "replication monitor", null, false)) {
            RecoverableZooKeeper rzk = masterZkw.getRecoverableZooKeeper();
            String rootNode = HConfiguration.getConfiguration().getSpliceRootPath();
            String node = rootNode + HConfiguration.MAX_RESERVED_TIMESTAMP_PATH;
            rzk.setData(node, Bytes.toBytes(ts), -1);
        }
    }
    private String electNewMaster() throws IOException, InterruptedException,
            KeeperException, SQLException, JSchException, ReplicationException{

        masterCluster = getMasterCluster();
        String newMaster = null;
        long maxTs = -1;
        peers = getReplicationPeers();
        for (String peer : peers) {
            String clusterKey = getClusterKey(peer);
            Configuration conf = ReplicationUtils.createConfiguration(clusterKey);
            long ts = ReplicationUtils.getTimestamp(conf, clusterKey);
            if (ts > maxTs) {
                maxTs = ts;
                newMaster = peer;
            }
        }
        return newMaster;
    }

    private List<String> getReplicationPeers() throws KeeperException, InterruptedException {
        RecoverableZooKeeper rzk = zkWatcher.getRecoverableZooKeeper();
        String path = replicationMonitorPath + "/peers";
        return rzk.exists(path, false) != null ? rzk.getChildren(path, false) : Lists.newArrayList();
    }

    private boolean replicationSyncUp() {

        return true;
    }

    private String getClusterKey(String node) {
        String[] s = node.split(":");
        String clusterKey = s[0]+":" + s[1]+":/"+s[2];
        return clusterKey;
    }

    private String executeScript(String command) throws  IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec(command);
        p.waitFor();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));



        String line = "";
        String error = "";
        while ((line = errorReader.readLine()) != null) {
            error += line;
        }

        if (error.length() > 0) {
            SpliceLogUtils.error(LOG, "Encountered an error when executing script %s : %s", healthcheckScript, error);
        }
        line = "";
        String result = "";
        while ((line = reader.readLine()) != null) {
            result += line;
        }

        return  result;
    }
}
