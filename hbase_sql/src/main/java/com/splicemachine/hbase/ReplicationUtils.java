package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Created by jyuan on 9/27/19.
 */
@SuppressFBWarnings(value = "MS_PKGPROTECT", justification = "intentional")
public class ReplicationUtils {

    private static final Logger LOG = Logger.getLogger(ReplicationUtils.class);

    public static final byte[] MASTER_CLUSTER_STATUS_DOWN = com.splicemachine.primitives.Bytes.toBytes(0);
    public static final byte[] MASTER_CLUSTER_STATUS_UP = com.splicemachine.primitives.Bytes.toBytes(1);
    private static final String DB_URL_LOCAL = "jdbc:splice://%s/splicedb;user=splice;password=admin";
    /**
     * Bump up timestamp if the provided timestamp value is larger than current timetamp
     * @param timestamp
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void setTimestamp(long timestamp) throws IOException, KeeperException, InterruptedException {
        TimestampSource timestampSource = SIDriver.driver().getTimestampSource();
        timestampSource.bumpTimestamp(timestamp);
        SpliceLogUtils.info(LOG, "bump up timestamp to %d", timestamp);
    }

    public static Configuration createConfiguration(String clusterKey) {
        Configuration conf = new Configuration();
        String[] s = clusterKey.split(":");
        String quorum = s[0];
        int port = Integer.parseInt(s[1]);
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.setInt("hbase.zookeeper.property.clientPort", port);
        if (s.length > 2) {
            conf.set("zookeeper.znode.parent", s[2]);
        }
        return conf;
    }

    public static List<String> getRegionServers(Configuration conf) throws IOException, InterruptedException, KeeperException{
        try(ZKWatcher zkWatcher = new ZKWatcher(conf, "replication monitor", null, false)) {
            RecoverableZooKeeper zk = zkWatcher.getRecoverableZooKeeper();
            List<String> servers = zk.getChildren("/splice/servers", false);
            return servers;
        }
    }

    public static Connection connect(String url) throws SQLException {
        Connection connection = DriverManager.getConnection(url, new Properties());
        LOG.info("Connected to " + url);
        return connection;
    }

    public static String getRegionServerAddress(Configuration conf) throws SQLException, IOException, InterruptedException, KeeperException{
        List<String> regionServers = getRegionServers(conf);
        for (String rs : regionServers) {
            try (Connection connection = connectToRegionServer(rs)) {
                return rs;
            }
            catch (SQLException e) {
                SpliceLogUtils.info(LOG, "Cannot connect to %s, retry another one", rs);
            }
        }
        return null;
    }

    public static long getTimestamp(Configuration conf, String clusterKey) throws IOException, InterruptedException, KeeperException, SQLException {
        try (Connection connection = connect(conf, clusterKey)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()")) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        }
    }

    public static Connection connect(Configuration conf, String clusterKey) throws IOException, InterruptedException, KeeperException, SQLException{
        List<String> regionServers = getRegionServers(conf);
        for (String rs : regionServers) {
            try {
                return connectToRegionServer(rs);
            } catch (Exception e) {
                SpliceLogUtils.warn(LOG, "not able to connect to %s. It may be down!!", rs);
            }
        }
        throw new RuntimeException("Not able to establish a JDBC connection to cluster" + clusterKey);
    }

    public static Connection connectToRegionServer(String hostAndPort) throws IOException, SQLException {
        String url = String.format(ReplicationUtils.DB_URL_LOCAL, hostAndPort);
        Connection connection = connect(url);
        return connection;
    }

    public static void addPeer(Connection connection,
                               String peerClusterKey,
                               String peerId,
                               boolean enabled) throws SQLException, IOException, KeeperException, InterruptedException {
        String sql = String.format("call syscs_util.add_peer(%s, '%s', '%s')", peerId, peerClusterKey,
                enabled ? "true" : "false");
        System.out.println("Run " + sql);
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                rs.next();
                try {
                    int index = rs.findColumn("Success");
                    String msg = rs.getString(index);
                    SpliceLogUtils.info(LOG, msg);
                    System.out.println(msg);
                } catch (SQLException e) {
                    String message = String.format("Failed to add a peer: %s : ", peerClusterKey, rs.getString(1));
                    SpliceLogUtils.error(LOG, message);
                    System.out.println(message);
                    throw e;
                }
            }
        }
    }

    public static void setupReplicationMaster(Connection connection, String clusterKey) throws SQLException {
        String sql = String.format("call syscs_util.set_replication_role('MASTER')");
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                rs.next();
                try {
                    int index = rs.findColumn("Success");
                    String msg = rs.getString(index);
                    SpliceLogUtils.info(LOG, msg);
                } catch (SQLException e) {
                    SpliceLogUtils.error(LOG, "Failed to setup master %s : ", clusterKey, rs.getString(1));
                    throw e;
                }
            }
        }

        //TODO - enable database replication
    }

    public static String getReplicationPath() {
        return HConfiguration.getConfiguration().getSpliceRootPath() +
                HConfiguration.getConfiguration().getReplicationPath();
    }

    public static String getReplicationSourcePath() {
        return getReplicationPath() + HConfiguration.DEFAULT_REPLICATION_SOURCE_PATH;
    }

    public static String getReplicationPeerPath() {
        return getReplicationPath() + HConfiguration.DEFAULT_REPLICATION_PEER_PATH;
    }

    public static void setReplicationRoleLocal(String role) throws IOException {
        SIDriver.driver().lifecycleManager().setReplicationRole(role);
        Collection<Context> allContexts=
                ContextService.getFactory().getAllContexts(LanguageConnectionContext.CONTEXT_ID);
        for(Context context : allContexts){
            ((LanguageConnectionContext)context).setReplicationRole(role);
        }
    }

    public static void setReplicationRole(String role) throws IOException {
        Txn txn = null;
        boolean prepared = false;
        SpliceTransactionResourceImpl transactionResource = null;
        try {
            txn = SIDriver.driver().lifecycleManager().beginTransaction();
            transactionResource = new SpliceTransactionResourceImpl();
            prepared = transactionResource.marshallTransaction(txn);
            TransactionController tc = transactionResource.getLcc().getTransactionExecute();
            DDLMessage.DDLChange change = ProtoUtil.createSetReplicationRole(txn.getTxnId(), role);
            String changeId = DDLUtils.notifyMetadataChange(change);
            tc.prepareDataDictionaryChange(changeId);
            tc.commitDataDictionaryChange();
            SpliceLogUtils.info(LOG, "Change replication role to %s", role);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (prepared)
                transactionResource.close();
        }
    }

    public static org.apache.hadoop.hbase.client.Connection createConnection(String clusterKey) throws IOException {
        String s[] = clusterKey.split(":");
        String zkQuorum = s[0];
        String port = s[1];
        String hbaseRoot = s[2];
        Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, hbaseRoot);
        org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory.createConnection(conf);
        return conn;
    }

    public static void disableMaster(String masterClusterKey) throws InterruptedException, KeeperException, IOException {

        // Delete all peers from master cluster
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
            System.out.println("Disabled peer " + peer);
        }
    }

    private static byte[] toByteArray(final ReplicationProtos.ReplicationState.State state) {
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
}
