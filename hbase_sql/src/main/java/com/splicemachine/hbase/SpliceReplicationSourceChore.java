package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.*;
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
import splice.com.google.common.collect.Lists;

import java.io.EOFException;
import java.io.FileNotFoundException;
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
                    Bytes.compareTo(status, HBaseConfiguration.REPLICATION_PRIMARY) == 0);

            Configuration configuration = HConfiguration.unwrapDelegate();
            hbaseRoot = configuration.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            String namespace = HConfiguration.getConfiguration().getNamespace();
            replicationProgressTableName = TableName.valueOf(namespace,
                    HConfiguration.REPLICA_REPLICATION_PROGRESS_TABLE_NAME);
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
            ConcurrentHashMap<String, SortedMap<String,Long>> snapshot = new ConcurrentHashMap<>();
            List<Future<Void>> futures = Lists.newArrayList();
            long timestamp = SIDriver.driver().getTimestampSource().currentTimestamp();
            if (timestamp != preTimestamp) {
                long currentTime = System.currentTimeMillis();
                for (PartitionServer server : servers) {
                    ServerName serverName = ServerName.valueOf(server.getHostname(), server.getPort(), server.getStartupTimestamp());
                    GetWALPositionsTask task = new GetWALPositionsTask(snapshot, serverName);
                    futures.add(executorService.submit(task));
                }
                for (Future<Void> future : futures) {
                    future.get();
                }

                if (LOG.isDebugEnabled()) {
                    List<String> sortedWals = sortWals(snapshot);
                    for (String wal:sortedWals )
                    SpliceLogUtils.debug(LOG, "snapshot %s", wal);
                }
                // Send Snapshots to each peer
                for (ReplicationPeerDescription peer : peers) {
                    if (peer.isEnabled()) {
                        String clusterKey = peer.getPeerConfig().getClusterKey();
                        Connection connection = getConnection(clusterKey);
                        removeOldWals(snapshot);
                        sendSnapshot(connection, timestamp, snapshot, currentTime);
                    }
                }
                preTimestamp = timestamp;
            }

            for (ReplicationPeerDescription peer : peers) {
                if (peer.isEnabled()) {
                    String clusterKey = peer.getPeerConfig().getClusterKey();
                    Connection connection = getConnection(clusterKey);
                    sendReplicationProgress(peer.getPeerId(), connection);
                }
            }

        }
        catch (Exception e) {
            SpliceLogUtils.error(LOG, "failed with exception: %s", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void sendReplicationProgress(String peerId, Connection connection) throws IOException {

        try {
            Map<String, SortedMap<String, Long>> walPositions = getWalPositions(peerId);
            if (LOG.isDebugEnabled()) {
                List<String> sortedWals = sortWals(walPositions);
                for (String wal:sortedWals )
                    SpliceLogUtils.debug(LOG, "replicated %s", wal);
            }
            //If there are more than one wal from a wal group, ignore old wals
            Set<String> oldWals = removeOldWals(walPositions);
            Map<String, Long> replicationProgress = getReplicationProgress(walPositions);
            updateReplicationProgress(replicationProgress, oldWals, connection);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @SuppressFBWarnings(value = "DM_DEFAULT_ENCODING", justification = "DB-9844")
    private void updateReplicationProgress(Map<String, Long> replicationProgress,
                                           Set<String> oldWals,
                                           Connection connection) throws IOException {

        try (Table table = connection.getTable(replicationProgressTableName)) {

            // Delete the current progress
            Delete delete = new Delete(HBaseConfiguration.REPLICATION_PROGRESS_ROWKEY_BYTES);
            table.delete(delete);

            long timestamp = System.currentTimeMillis();

            // Create a single row update containing
            Put put = new Put(HBaseConfiguration.REPLICATION_PROGRESS_ROWKEY_BYTES);

            // Add a timestamp column
            put.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,
                    HBaseConfiguration.REPLICATION_PROGRESS_TSCOL_BYTES, Bytes.toBytes(timestamp));

            // Add a column for each wal
            for(Map.Entry<String, Long> progress: replicationProgress.entrySet()){
                String wal = progress.getKey();
                Long position = progress.getValue();
                byte[] walCol = wal.getBytes();

                //if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.info(LOG, "updateReplicationProgress: wal = %s, position = %d", wal, position);
                //}

                put.addColumn(SIConstants.DEFAULT_FAMILY_BYTES, walCol,
                        Bytes.toBytes(position));
            }
            table.put(put);

        }
    }

    @SuppressFBWarnings(value = "WMI_WRONG_MAP_ITERATOR", justification = "DB-9844")
    private Map<String, Long> getReplicationProgress(Map<String, SortedMap<String, Long>> serverWalPositions) throws IOException {

        Configuration conf = HConfiguration.unwrapDelegate();
        FileSystem fs = FSUtils.getWALFileSystem(conf);
        Map<String, Long> replicationProgress = new HashMap<>();
        PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
        for (String server : serverWalPositions.keySet()) {
            Map<String, Long> walPosition = serverWalPositions.get(server);
            for (String wal : walPosition.keySet()) {

                long position = walPosition.get(wal);
                WALLink walLink = null;
                try {
                    walLink = new WALLink(conf, server, wal);
                } catch (FileNotFoundException e) {
                    SpliceLogUtils.warn(LOG, "WAL %s no longer exists", wal);
                    replicationProgress.put(wal, position);
                    continue;
                }

                try (WAL.Reader reader = WALFactory.createReader(fs, walLink.getAvailablePath(fs), conf)) {
                    // Seek to the position and look ahead
                    if (position > 0) {
                        reader.seek(position);
                    }
                    WAL.Entry entry;
                    String family = null;
                    long previous = position;
                    while ((entry = reader.next()) != null) {
                        WALEdit edit = entry.getEdit();
                        WALKey walKey = entry.getKey();
                        TableName table = walKey.getTableName();
                        String ns = table.getNamespaceAsString();
                        String name = table.getQualifierAsString();
                        if (!ns.equals(HConfiguration.getConfiguration().getNamespace())) {
                            // ignore non-splice table
                            previous = reader.getPosition();
                            SpliceLogUtils.info(LOG, "Skip non-splice table %s.%s. Bump up position to %d",
                                    ns, name, previous);
                            continue;
                        }
                        if (!admin.replicationEnabled(name)) {
                            previous = reader.getPosition();
                            SpliceLogUtils.info(LOG, "Skip non-replicated table %s.%s. Bump up position to %d",
                                    ns, name, previous);
                            continue;
                        }

                        List<Cell> cells = edit.getCells();
                        if (cells.size() > 0) {
                            Cell cell = cells.get(0);
                            family = Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(),
                                    cell.getFamilyLength());
                            String row = Bytes.toStringBinary(cell.getRowArray(), cell.getRowOffset(),
                                    cell.getRowLength());
                            if (family.equals(SIConstants.DEFAULT_FAMILY_NAME))
                                break;
                            else {
                                previous = reader.getPosition();
                                SpliceLogUtils.info(LOG, "See a cell of %s from %s. Bump up position to %d", family,
                                        walLink.getAvailablePath(fs), previous);
                            }
                        }
                    }
                    if (family == null) {
                        replicationProgress.put(wal, reader.getPosition());
                    }
                    else {
                        replicationProgress.put(wal, previous);
                    }
                    if (LOG.isDebugEnabled()) {
                        SpliceLogUtils.debug(LOG, "WAL = %s, position = %d", wal, previous);
                    }
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
    private Map<String, SortedMap<String, Long>> getWalPositions(String peerId) throws IOException {
        try {
            Map<String, SortedMap<String, Long>> serverWalPositionsMap = new HashMap<>();
            String rsPath = hbaseRoot + "/" + "replication/rs";
            List<String> regionServers = ZkUtils.getChildren(rsPath, false);
            for (String rs : regionServers) {
                String peerPath = rsPath + "/" + rs + "/" + peerId;
                List<String> walNames = ZkUtils.getChildren(peerPath, false);
                SortedMap<String, Long>  walPositionsMap = new TreeMap<>();
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
    @SuppressFBWarnings(value = "DM_NUMBER_CTOR", justification = "DB-9844")
    private Set<String> removeOldWals(Map<String, SortedMap<String, Long>> serverWalPositionsMap) {
        Map<String, Long> regionGroupMap = new HashMap<>();
        SortedMap<String, Long> copy = new TreeMap<>();
        Set<String> oldWals = new HashSet<>();
        for (SortedMap<String, Long> walPositions : serverWalPositionsMap.values()) {
            copy.clear();
            copy.putAll(walPositions);

            // If there are more than 1 wal from a region group, ignore old wal
            for (Map.Entry<String, Long> entry : copy.entrySet()) {
                long position = entry.getValue();
                String walName = entry.getKey();
                int index = walName.lastIndexOf(".");
                String walGroup = walName.substring(0, index);
                Long logNum = new Long(walName.substring(index + 1));
                if (regionGroupMap.containsKey(walGroup)) {
                    Long ln = regionGroupMap.get(walGroup);
                    if (logNum > ln && position > 0) {
                        regionGroupMap.put(walGroup, logNum);
                        String key = walGroup + "." + ln;
                        walPositions.remove(key);
                        oldWals.add(key);
                        SpliceLogUtils.debug(LOG, "Log %s:%d has completed replication, remove it", key, walPositions.get(key));
                    } else {
                        walPositions.remove(walName);
                        oldWals.add(walName);
                        SpliceLogUtils.debug(LOG, "Ignore log %s:%d because it has not bee replicated", walName, walPositions.get(walName));
                    }
                } else {
                    regionGroupMap.put(walGroup, logNum);
                }
            }
        }
        return oldWals;
    }

    /**
     * Insert a row to splice:SPLICE_MASTER_SNAPSHOTS for each replica cluster
     * @param conn
     * @param timestamp
     * @param serverSnapshot
     * @throws Exception
     */
    @SuppressFBWarnings(value = "DM_DEFAULT_ENCODING", justification = "DB-9844")
    private void sendSnapshot(Connection conn,
                              Long timestamp,
                              ConcurrentHashMap<String, SortedMap<String, Long>> serverSnapshot,
                              long currentTime) throws Exception{

        Table table = null;
        try {
            String namespace = HConfiguration.getConfiguration().getNamespace();
            table = conn.getTable(TableName.valueOf(namespace + ":" + HConfiguration.MASTER_SNAPSHOTS_TABLE_NAME));

            // construct the single put
            byte[] rowKey = timestamp.toString().getBytes();
            Put put = new Put(rowKey);

            put.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,
                    HBaseConfiguration.REPLICATION_SNAPSHOT_TSCOL_BYTES, Bytes.toBytes(currentTime));

            StringBuffer sb = new StringBuffer();
            // add encoded region column into the put
            for (Map<String, Long> snapshot : serverSnapshot.values()) {
                for (HashMap.Entry<String, Long> entry : snapshot.entrySet()) {
                    put.addColumn(cf, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                    if (LOG.isDebugEnabled()) {
                        sb.append(String.format("{WAL=%s, position=%d}", entry.getKey(), entry.getValue()));
                    }
                }
            }
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "ts = %d, WALs = %s", timestamp, sb.toString());
            }
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
            conn = ReplicationUtils.createConnection(clusterKey);
            connectionMap.put(clusterKey, conn);
        }
        return conn;
    }

    private void initReplicationConfig() throws IOException {
        SpliceLogUtils.info(LOG, "isReplicationMaster = %s", isReplicationMaster);
        if (isReplicationMaster) {
            //ReplicationUtils.setReplicationRole("PRIMARY");
        }
    }
    public void changeStatus() throws IOException{
        byte[] status = ZkUtils.getData(replicationPath, new ReplicationMasterWatcher(this), null);
        boolean wasReplicationMaster = isReplicationMaster;
        isReplicationMaster = Bytes.compareTo(status, HBaseConfiguration.REPLICATION_PRIMARY) == 0;
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

    private List<String> sortWals(Map<String, SortedMap<String, Long>> wals) {
        List<String> sortedWals = Lists.newArrayList();
        for ( Map<String, Long> w : wals.values()) {
            for (Map.Entry<String, Long> walPosition : w.entrySet()) {
                sortedWals.add(walPosition.getKey()+":"+walPosition.getValue());
            }
        }

        Collections.sort(sortedWals);
        return sortedWals;
    }
}
