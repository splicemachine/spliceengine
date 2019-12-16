package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.WALLink;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.spark_project.guava.collect.Lists;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by jyuan on 8/1/19.
 */
public class SpliceReplicationSourceChore extends ScheduledChore {

    private static final Logger LOG = Logger.getLogger(SpliceReplicationSourceChore.class);

    final static byte[] cf = SIConstants.DEFAULT_FAMILY_BYTES;

    private Admin admin;
    private long preTimestamp;
    private String replicationPath;
    private volatile boolean isReplicationMaster;
    private volatile boolean statusChanged;
    private RecoverableZooKeeper rzk;
    private Map<String, Connection> connectionMap = new HashedMap();
    private String hbaseRoot;
    private TableName replicationProgressTableName;

    public SpliceReplicationSourceChore(final String name, final Stoppable stopper, final int period) throws IOException {
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

            Configuration configuration = HConfiguration.unwrapDelegate();
            hbaseRoot = configuration.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            String namespace = HConfiguration.getConfiguration().getNamespace();
            replicationProgressTableName = TableName.valueOf(namespace,
                    HConfiguration.SLAVE_REPLICATION_PROGRESS_TABLE_NAME);
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
            ConcurrentHashMap<String, Map<String,Long>> snapshot = new ConcurrentHashMap<>();
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
                    Connection connection = getConnection(clusterKey);
                    removeOldWals(snapshot);
                    sendSnapshot(connection, timestamp, snapshot);
                    sendReplicationProgress(peer.getPeerId(), connection);
                }
                preTimestamp = timestamp;
            }

        }
        catch (Exception e) {
            SpliceLogUtils.error(LOG, "failed with exception: %s", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void sendReplicationProgress(String peerId, Connection connection) throws IOException {

        try {
            Map<String, Map<String, Long>> walPositions = getWalPositions(peerId);
            //If there are more than one wal from a wal group, ignore old wals
            Set<String> oldWals = removeOldWals(walPositions);
            Map<String, Long> replicationProgress = getReplicationProgress(walPositions);
            updateReplicationProgress(replicationProgress, oldWals, connection);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void updateReplicationProgress(Map<String, Long> replicationProgress,
                                           Set<String> oldWals,
                                           Connection connection) throws IOException {

        try (Table table = connection.getTable(replicationProgressTableName)) {

            long timestamp = System.currentTimeMillis();

            // Create a single row update containing
            Put put = new Put(com.splicemachine.access.configuration.HBaseConfiguration.REPLICATION_PROGRESS_ROWKEY_BYTES);

            // Add a timestamp column
            put.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,
                    com.splicemachine.access.configuration.HBaseConfiguration.REPLICATION_PROGRESS_TSCOL_BYTES, Bytes.toBytes(timestamp));

            // Add a column for each wal
            for(Map.Entry<String, Long> progress: replicationProgress.entrySet()){
                String wal = progress.getKey();
                Long position = progress.getValue();
                byte[] walCol = wal.getBytes();

                //if (LOG.isDebugEnabled()) {
                SpliceLogUtils.info(LOG, "updateReplicationProgress: wal = %s, position = %d", wal, position);
                //}
                if (position > 0) {
                    put.addColumn(SIConstants.DEFAULT_FAMILY_BYTES, walCol,
                            Bytes.toBytes(position));
                }
            }
            table.put(put);

            Delete delete = new Delete(com.splicemachine.access.configuration.HBaseConfiguration.REPLICATION_PROGRESS_ROWKEY_BYTES);
            for (String oldWal : oldWals) {
                delete.addColumn(SIConstants.DEFAULT_FAMILY_BYTES, oldWal.getBytes());
            }
            table.delete(delete);
        }
    }

    private Map<String, Long> getReplicationProgress(Map<String, Map<String, Long>> serverWalPositions) throws IOException {

        Configuration conf = HConfiguration.unwrapDelegate();
        FileSystem fs = FSUtils.getWALFileSystem(conf);
        Map<String, Long> replicationProgress = new HashMap<>();

        for (String server : serverWalPositions.keySet()) {
            Map<String, Long> walPosition = serverWalPositions.get(server);
            for (String wal : walPosition.keySet()) {

                WALLink walLink = new WALLink(conf, server, wal);
                long position = walPosition.get(wal);

//                long len = fs.getFileStatus(walLink.getAvailablePath(fs)).getLen();
//                SpliceLogUtils.info(LOG, "WAL = %s, position = %d, len = %d", wal, position, len);
//
//                if (position == len) {
//                    SpliceLogUtils.info(LOG, "WAL = %s, position = %d", wal, position);
//                    replicationProgress.put(wal, position);
//                    continue;
//                }

                try (WAL.Reader reader = WALFactory.createReader(fs, walLink.getAvailablePath(fs), conf)) {
                    // Seek to the position and look ahead
                    reader.seek(position);
                    WAL.Entry entry;
                    long previous = position;
                    while ((entry = reader.next()) != null) {
                        WALEdit edit = entry.getEdit();

                        List<Cell> cells = edit.getCells();
                        if (cells.size() > 0) {
                            Cell cell = cells.get(0);
                            String family = Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(),
                                    cell.getFamilyLength());
                            if (family.equals(SIConstants.DEFAULT_FAMILY_NAME))
                                break;
                            else {
                                previous = reader.getPosition();
                                SpliceLogUtils.info(LOG, "See a cell of %s. Bump up position to %d", family, previous);
                            }
                        }
                    }
                    replicationProgress.put(wal, previous);
                    SpliceLogUtils.info(LOG, "WAL = %s, position = %d", wal, previous);
                }
                catch (EOFException e) {
                    replicationProgress.put(wal, position);
                }
            }
        }
        return  replicationProgress;
    }

    /**
     * Get Wal positions for a replication peer
     * @param peerId
     * @return
     * @throws IOException
     */
    private Map<String, Map<String, Long>> getWalPositions(String peerId) throws IOException {
        try {
            Map<String, Map<String, Long>> serverWalPositionsMap = new HashMap<>();
            String rsPath = hbaseRoot + "/" + "replication/rs";
            List<String> regionServers = ZkUtils.getChildren(rsPath, false);
            for (String rs : regionServers) {
                String peerPath = rsPath + "/" + rs + "/" + peerId;
                List<String> walNames = ZkUtils.getChildren(peerPath, false);
                Map<String, Long>  walPositionsMap = new HashMap<>();
                serverWalPositionsMap.put(rs, walPositionsMap);
                for (String walName : walNames) {
                    byte[] p = ZkUtils.getData(peerPath + "/" + walName);
                    long position = ZKUtil.parseWALPositionFrom(p);
                    walPositionsMap.put(walName, position);
                    if (LOG.isDebugEnabled()) {
                        SpliceLogUtils.debug(LOG, "WAL=%s, position=%d", walName, position);
                    }
                }
            }
            return serverWalPositionsMap;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Remove old Wals from each wal group
     * @param serverWalPositionsMap
     */
    private Set<String> removeOldWals(Map<String, Map<String, Long>> serverWalPositionsMap) {
        Map<String, Long> regionGroupMap = new HashMap<>();
        Map<String, Long> copy = new HashMap<>();
        Set<String> oldWals = new HashSet<>();
        for (Map<String, Long> walPositions : serverWalPositionsMap.values()) {
            copy.putAll(walPositions);

            // If there are more than 1 wal from a region group, ignore old wal
            for (Map.Entry<String, Long> entry : copy.entrySet()) {
                String walName = entry.getKey();
                int index = walName.lastIndexOf(".");
                String walGroup = walName.substring(0, index);
                Long logNum = new Long(walName.substring(index + 1));
                if (regionGroupMap.containsKey(walGroup)) {
                    Long ln = regionGroupMap.get(walGroup);
                    if (logNum > ln) {
                        regionGroupMap.put(walGroup, logNum);
                        String key = walGroup + "." + ln;
                        walPositions.remove(key);
                        oldWals.add(key);
                        SpliceLogUtils.info(LOG, "Log %s:%d has completed replication, remove it", key, walPositions.get(key));
                    } else {
                        walPositions.remove(walName);
                        oldWals.add(walName);
                        SpliceLogUtils.info(LOG, "Log %s:%d has completed replication, remove it", walName, walPositions.get(walName));
                    }
                } else {
                    regionGroupMap.put(walGroup, logNum);
                }
            }
        }
        return oldWals;
    }

    /**
     * Insert a row to splice:SPLICE_MASTER_SNAPSHOTS for each slave cluster
     * @param conn
     * @param timestamp
     * @param serverSnapshot
     * @throws Exception
     */
    private void sendSnapshot(Connection conn, Long timestamp, ConcurrentHashMap<String, Map<String, Long>> serverSnapshot) throws Exception{

        Table table = null;
        try {
            String namespace = HConfiguration.getConfiguration().getNamespace();
            table = conn.getTable(TableName.valueOf(namespace + ":" + HConfiguration.MASTER_SNAPSHOTS_TABLE_NAME));

            // construct the single put
            byte[] rowKey = timestamp.toString().getBytes();
            Put put = new Put(rowKey);
            StringBuffer sb = new StringBuffer();
            // add encoded region column into the put
            for (Map<String, Long> snapshot : serverSnapshot.values()) {
                for (HashMap.Entry<String, Long> entry : snapshot.entrySet()) {
                    put.addColumn(cf, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                    //if (LOG.isDebugEnabled()) {
                    sb.append(String.format("{WAL=%s, position=%d}", entry.getKey(), entry.getValue()));
                    //}
                }
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

    private Connection getConnection(String clusterKey) throws IOException {
        Connection conn = null;
        if (connectionMap.containsKey(clusterKey)) {
            conn = connectionMap.get(clusterKey);
        }
        else {
            String s[] = clusterKey.split(":");
            String zkQuorum = s[0];
            String port = s[1];
            String hbaseRoot = s[2];
            Configuration conf = HBaseConfiguration.create();
            conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);
            conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, hbaseRoot);
            conn = ConnectionFactory.createConnection(conf);
            connectionMap.put(clusterKey, conn);
        }
        return conn;
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
        private final SpliceReplicationSourceChore replicationSnapshotTrackerChore;

        public ReplicationMasterWatcher(SpliceReplicationSourceChore replicationSnapshotTrackerChore) {
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
