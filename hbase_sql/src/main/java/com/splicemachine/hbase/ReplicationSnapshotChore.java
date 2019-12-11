package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.spark_project.guava.collect.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by jyuan on 8/1/19.
 */
public class ReplicationSnapshotChore extends ScheduledChore {

    private static final Logger LOG = Logger.getLogger(ReplicationSnapshotChore.class);

    final static byte[] cf = SIConstants.DEFAULT_FAMILY_BYTES;

    private Admin admin;
    private long preTimestamp;
    private String replicationPath;
    private volatile boolean isReplicationMaster;
    private volatile boolean statusChanged;
    private RecoverableZooKeeper rzk;

    public ReplicationSnapshotChore(final String name, final Stoppable stopper, final int period) throws IOException {
        super(name, stopper, period);
        init();
    }

    private void init() throws IOException {
        try {
            SpliceLogUtils.info(LOG, "init()");
            SConfiguration conf = HConfiguration.getConfiguration();
            Connection conn = HBaseConnectionFactory.getInstance(conf).getConnection();
            admin = conn.getAdmin();
            replicationPath = ReplicationUtils.getReplicationPath();
            rzk = ZkUtils.getRecoverableZooKeeper();
            while (rzk.exists(replicationPath, false) ==null) {
                Thread.sleep(100);
            }
            byte[] status = rzk.getData(replicationPath, new ReplicationMasterWatcher(this), null);
            isReplicationMaster = (status != null &&
                    Bytes.compareTo(status, com.splicemachine.access.configuration.HBaseConfiguration.REPLICATION_MASTER) == 0);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void chore() {
        try {

            if (statusChanged) {
                SpliceLogUtils.info(LOG, "status changed");
                initReplicationConfig();
                statusChanged = false;
            }

            if (!isReplicationMaster)
                return;

            List<ReplicationPeerDescription> peers = admin.listReplicationPeers();
            if (peers.size() == 0) {
                return;
            }

            PartitionAdmin pa = SIDriver.driver().getTableFactory().getAdmin();
            ExecutorService executorService = SIDriver.driver().getExecutorService();
            Collection<PartitionServer> servers = pa.allServers();

            // Get LSNs for each region server
            ConcurrentHashMap<String, Long> snapshot = new ConcurrentHashMap<>();
            List<Future<Void>> futures = Lists.newArrayList();
            long timestamp = SIDriver.driver().getTimestampSource().currentTimestamp();
            if (timestamp != preTimestamp) {

                for (PartitionServer server : servers) {
                    ServerName serverName = ServerName.valueOf(server.getHostname(), server.getPort(), server.getStartupTimestamp());
                    GetWALPositionsTask task = new GetWALPositionsTask(snapshot, serverName);
                    futures.add(executorService.submit(task));
                }
                for (Future<Void> future : futures) {
                    future.get();
                }

                // Send Snapshots to each peer
                for (ReplicationPeerDescription peer : peers) {
                    String clusterKey = peer.getPeerConfig().getClusterKey();
                    updateSlaveTable(clusterKey, timestamp, snapshot);
                }
                preTimestamp = timestamp;
            }

        }
        catch (Exception e) {
            SpliceLogUtils.error(LOG, "failed with exception: %s", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Insert a row to splice:SPLICE_MASTER_SNAPSHOTS for each slave cluster
     * @param clusterKey
     * @param timestamp
     * @param snapshot
     * @throws Exception
     */
    private void updateSlaveTable(String clusterKey, Long timestamp, ConcurrentHashMap<String, Long> snapshot) throws Exception{

        String s[] = clusterKey.split(":");
        String zkQuorum = s[0];
        String port = s[1];
        String hbaseRoot = s[2];
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, hbaseRoot);
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = null;
        try {
            String namespace = HConfiguration.getConfiguration().getNamespace();
            table = conn.getTable(TableName.valueOf(namespace + ":" + HConfiguration.MASTER_SNAPSHOTS_TABLE_NAME));

            // construct the single put
            byte[] rowKey = timestamp.toString().getBytes();
            Put put = new Put(rowKey);
            StringBuffer sb = new StringBuffer();
            // add encoded region column into the put
            for (HashMap.Entry<String, Long> entry : snapshot.entrySet()) {
                put.addColumn(cf, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                //if (LOG.isDebugEnabled()) {
                    sb.append(String.format("{WAL=%s, position=%d}", entry.getKey(), entry.getValue()));
                //}
            }
            //if (LOG.isDebugEnabled()) {
                SpliceLogUtils.info(LOG, "ts = %d, WALs = %s", timestamp, sb.toString());
            //}
            table.put(put);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        finally {
            if (table != null)
                table.close();
        }
    }

    private void initReplicationConfig() throws IOException {
        SpliceLogUtils.info(LOG, "isReplicationMaster = %s", isReplicationMaster);
        if (isReplicationMaster) {
            //ReplicationUtils.setReplicationRole("MASTER");
        }
    }
    public void changeStatus() throws IOException{
        byte[] status = ZkUtils.getData(replicationPath, new ReplicationMasterWatcher(this), null);
        boolean wasReplicationMaster = isReplicationMaster;
        isReplicationMaster = Bytes.compareTo(status, com.splicemachine.access.configuration.HBaseConfiguration.REPLICATION_MASTER) == 0;
        SpliceLogUtils.info(LOG, "isReplicationMaster changes from %s to %s", wasReplicationMaster, isReplicationMaster);
        statusChanged = wasReplicationMaster!=isReplicationMaster;
    }

    private static class ReplicationMasterWatcher implements Watcher {
        private final ReplicationSnapshotChore replicationSnapshotTrackerChore;

        public ReplicationMasterWatcher(ReplicationSnapshotChore replicationSnapshotTrackerChore) {
            this.replicationSnapshotTrackerChore = replicationSnapshotTrackerChore;
        }

        @Override
        public void process(WatchedEvent event) {
            Event.EventType type = event.getType();
            try {
                if (type == Event.EventType.NodeDataChanged) {
                    replicationSnapshotTrackerChore.changeStatus();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
