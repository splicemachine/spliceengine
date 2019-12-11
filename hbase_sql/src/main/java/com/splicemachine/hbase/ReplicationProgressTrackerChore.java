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

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.*;

/**
 * Created by jyuan on 8/1/19.
 */
public class ReplicationProgressTrackerChore extends ScheduledChore {

    private static final Logger LOG = Logger.getLogger(ReplicationProgressTrackerChore.class);
    Connection connection;
    private Map<String, Long> replicationProgress = new HashMap<>();
    private TableName masterSnapshotTable;
    private RecoverableZooKeeper rzk;
    private String replicationPath;
    private String replicationPeerPath;
    private String replicationSourcePath;
    private volatile boolean isReplicationSlave;
    private ZKWatcher replicationSourceWatcher;
    private String peerId;
    private String masterQuorum;
    private String rootDir;
    private volatile boolean statusChanged = false;

    public ReplicationProgressTrackerChore(final String name, final Stoppable stopper, final int period) throws IOException {
        super(name, stopper, period);
        init();
    }

    private void init() throws IOException {
        try {
            SpliceLogUtils.info(LOG, "init()");
            connection = HBaseConnectionFactory.getInstance(HConfiguration.getConfiguration()).getConnection();
            String namespace = HConfiguration.getConfiguration().getNamespace();
            masterSnapshotTable = TableName.valueOf(namespace, HBaseConfiguration.MASTER_SNAPSHOTS_TABLE_NAME);
            rzk = ZkUtils.getRecoverableZooKeeper();
            replicationPath = ReplicationUtils.getReplicationPath();
            replicationPeerPath = ReplicationUtils.getReplicationPeerPath();
            replicationSourcePath = ReplicationUtils.getReplicationSourcePath();
            while (rzk.exists(replicationPath, false) ==null) {
                Thread.sleep(100);
            }
            byte[] status = rzk.getData(replicationPath, new ReplicationSlaveWatcher(this), null);
            isReplicationSlave = Bytes.compareTo(status, HBaseConfiguration.REPLICATION_SLAVE) == 0;
            initReplicationConfig();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void chore() {
        try {

            if (statusChanged) {
                SpliceLogUtils.info(LOG, "status changed");
                initReplicationConfig();
                //ReplicationUtils.setReplicationRole("SLAVE");
                statusChanged = false;
            }

            if (!isReplicationSlave)
                return;


            if (replicationProgress.size() == 0) {
                getReplicationProgress(replicationProgress);
                cleanupReplicationProgress(replicationProgress);
                // If there is no entry in SLAVE_REPLICATION_PROGRESS, do nothing
                if (replicationProgress.size() == 0)
                    return;
            }
            updateProgress();
        } catch (TableNotFoundException e) {
            // Ignore TableNotFoundException during database creation
            SpliceLogUtils.warn(LOG, "Unexpected exception", e);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, "Unexpected exception", e);
            throw new RuntimeException(e);
        }
    }

    private void cleanupReplicationProgress(Map<String, Long> replicationProgress) {
        Map<String, Long> regionGroupMap = new HashMap<>();
        Map<String, Long> copy = new HashMap<>();
        copy.putAll(replicationProgress);

        // If there are more than 1 wal from a region group, ignore old wal
        for (Map.Entry<String, Long> entry : copy.entrySet()) {
            String walName = entry.getKey();
            int index = walName.lastIndexOf(".");
            String walGroup = walName.substring(0, index);
            Long logNum = new Long(walName.substring(index+1));
            if (regionGroupMap.containsKey(walGroup)) {
                Long ln = regionGroupMap.get(walGroup);
                if (logNum > ln) {
                    regionGroupMap.put(walGroup, logNum);
                    String key = walGroup+ "." + ln;
                    replicationProgress.remove(key);
                    SpliceLogUtils.info(LOG, "Log %s has completed replication, remove it", key);
                }
                else {
                    replicationProgress.remove(walName);
                    SpliceLogUtils.info(LOG, "Log %s has completed replication, remove it", walName);
                }
            }
            else {
                regionGroupMap.put(walGroup, logNum);
            }
        }

        copy.clear();
        copy.putAll(replicationProgress);

        // Ignore wal that has not been replicated
        for(String key : copy.keySet()) {
            long position = replicationProgress.get(key);
            if (position == 0) {
                replicationProgress.remove(key);
            }
        }
    }
    private void getReplicationProgress(Map<String, Long> replicationProgress) throws IOException {

        try {
            RecoverableZooKeeper rzk = replicationSourceWatcher.getRecoverableZooKeeper();
            List<String> regionServers = rzk.getChildren(rootDir + "/replication/rs", false);
            for (String rs: regionServers) {
                String path = rootDir + "/replication/rs/" + rs + "/" + peerId;
                if (rzk.exists(path, false) != null) {
                    List<String> fileNames = rzk.getChildren(path, false);
                    for (String fileName : fileNames) {
                        try {
                            byte[] pos = rzk.getData(path + "/" + fileName, false, null);
                            long position = ZKUtil.parseWALPositionFrom(pos);
                            replicationProgress.put(fileName, position);
                        } catch (KeeperException.NoNodeException ne) {
                            SpliceLogUtils.info(LOG, "Node %s does not exists because the log has completed " +
                                    "replication. Ignore...");
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void changeStatus() throws IOException{
        byte[] status = ZkUtils.getData(replicationPath, new ReplicationSlaveWatcher(this), null);
        if (Bytes.compareTo(status, HBaseConfiguration.REPLICATION_NONE) == 0) {
            //ReplicationUtils.setReplicationRole("NONE");
        }
        boolean wasReplicationSlave = isReplicationSlave;
        isReplicationSlave = Bytes.compareTo(status, HBaseConfiguration.REPLICATION_SLAVE) == 0;
        SpliceLogUtils.info(LOG, "isReplicationSlave changed from %s to %s", wasReplicationSlave, isReplicationSlave);
        statusChanged = wasReplicationSlave!=isReplicationSlave;
    }

    private void initReplicationConfig() throws IOException {
        SpliceLogUtils.info(LOG, "isReplicationSlave = %s", isReplicationSlave);
        if (isReplicationSlave) {
            String clusterKey = new String(ZkUtils.getData(replicationSourcePath));
            peerId = new String(ZkUtils.getData(replicationPeerPath));
            String[] s = clusterKey.split(":");
            masterQuorum = s[0] + ":" + s[1];
            rootDir = s[2];
            Configuration masterConf = ReplicationUtils.createConfiguration(masterQuorum);
            replicationSourceWatcher = new ZKWatcher(masterConf, "replicationProgressTrackerChore", null, false);
        }
        else {
            if (replicationSourceWatcher != null) {
                replicationSourceWatcher.close();
                replicationSourceWatcher = null;
                rootDir = null;
                peerId = null;
            }
        }
    }
    /**
     * Try to update replication progress
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void updateProgress() throws IOException, KeeperException, InterruptedException {
        Table snapshotTable = connection.getTable(masterSnapshotTable);
        Scan scan = new Scan();
        try (ResultScanner scanner = snapshotTable.getScanner(scan)) {
            for (Result r : scanner) {
                byte[] rowKey = r.getRow();
                long timestamp = new Long(new String(rowKey));
                //if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.info(LOG, "Checking snapshot taken at %d", timestamp);
                //}
                CellScanner s = r.cellScanner();
                while (s.advance()) {
                    Cell cell = s.current();
                    String walName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    Long position = Bytes.toLong(CellUtil.cloneValue(cell));
                    if (replicationProgress.containsKey(walName)) {
                        long appliedPosition = replicationProgress.get(walName);
                        //if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.info(LOG,
                                    "WAL=%s, snapshot=%d, progress=%d", walName, position, appliedPosition);
                        //}
                        if (appliedPosition < position) {
                            // applied seqNum is behind snapshot seqNum,cannot move timestamp forward
                            return;
                        }
                    }
                }
                Delete d = new Delete(rowKey);
                // We have replicated beyond this snapshot, delete it and bump up timestamp
                snapshotTable.delete(d);
                //if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.info(LOG, "Deleted snapshot %d.", timestamp);
                //}
                ReplicationUtils.setTimestamp(timestamp);
            }
        }finally {
            replicationProgress.clear();
        }
    }

    private static class ReplicationSlaveWatcher implements Watcher {
        private final ReplicationProgressTrackerChore replicationProgressTrackerChore;

        public ReplicationSlaveWatcher(ReplicationProgressTrackerChore replicationProgressTrackerChore) {
            this.replicationProgressTrackerChore = replicationProgressTrackerChore;
        }

        @Override
        public void process(WatchedEvent event) {
            Event.EventType type = event.getType();
            try {
                if (type == Event.EventType.NodeDataChanged) {
                    replicationProgressTrackerChore.changeStatus();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
