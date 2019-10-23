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
import org.apache.log4j.Logger;
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

    private ConcurrentHashMap<String, Long> previousSnapshot;
    private Admin admin;
    private long preTimestamp;

    public ReplicationSnapshotChore(final String name, final Stoppable stopper, final int period) throws IOException {
        super(name, stopper, period);
        init();
    }

    private void init() throws IOException {
        SConfiguration conf = HConfiguration.getConfiguration();
        Connection conn = HBaseConnectionFactory.getInstance(conf).getConnection();
        admin = conn.getAdmin();
    }

    @Override
    protected void chore() {
        try {
            boolean replicationEnabled = HConfiguration.getConfiguration().replicationEnabled();
            if (!replicationEnabled)
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
                    GetLSNTask task = new GetLSNTask(snapshot, serverName);
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
                previousSnapshot = snapshot;
                preTimestamp = timestamp;
            }

        }
        catch (Exception e) {
            SpliceLogUtils.error(LOG, "failed with exception: %s", e.getMessage());
            throw new RuntimeException(e);
        }
    }
    /**
     * Whether there were changes between two snapshots
     * @param previousSnapshot
     * @param snapshot
     * @return
     */
    private boolean snapshotChanged(ConcurrentHashMap<String, Long> previousSnapshot,
                                    ConcurrentHashMap<String, Long> snapshot) {

        if (previousSnapshot == null) {
            return snapshot.size() > 0;
        }

        if (previousSnapshot.size() != snapshot.size()) {
            return true;
        }

        for (String key : snapshot.keySet()) {
            if (!previousSnapshot.containsKey(key))
                return true;

            long value = snapshot.get(key);
            long oldValue = previousSnapshot.get(key);
            if (value != oldValue)
                return true;
        }

        return false;
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
                    sb.append(String.format("{region=%s, LSN=%d}", entry.getKey(), entry.getValue()));
                //}
            }
            //if (LOG.isDebugEnabled()) {
                SpliceLogUtils.info(LOG, "ts = %d, LSNs = %s", timestamp, sb.toString());
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
}
