package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.configuration.ConfigurationSource;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Created by jyuan on 9/27/19.
 */
public class ReplicationUtils {

    private static final Logger LOG = Logger.getLogger(ReplicationUtils.class);

    public static final byte[] MASTER_CLUSTER_STATUS_DOWN = com.splicemachine.primitives.Bytes.toBytes(0);
    public static final byte[] MASTER_CLUSTER_STATUS_UP = com.splicemachine.primitives.Bytes.toBytes(1);
    public static final String DB_URL_LOCAL = "jdbc:splice://%s/splicedb;user=splice;password=admin";
    /**
     * Bump up timestamp if the provided timestamp value is larger than current timetamp
     * @param timestamp
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void setTimestamp(long timestamp) throws IOException, KeeperException, InterruptedException {
        TimestampSource timestampSource = SIDriver.driver().getTimestampSource();
        long currentTimestamp = timestampSource.currentTimestamp();
        if (currentTimestamp < timestamp) {
            RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
            HBaseSIEnvironment env = HBaseSIEnvironment.loadEnvironment(new SystemClock(), rzk);
            ConfigurationSource configurationSource = env.configuration().getConfigSource();
            String rootNode = configurationSource.getString(HConfiguration.SPLICE_ROOT_PATH, HConfiguration.DEFAULT_ROOT_PATH);
            String node = rootNode + HConfiguration.MAX_RESERVED_TIMESTAMP_PATH;
            //if (LOG.isDebugEnabled()) {
            SpliceLogUtils.info(LOG, "bump up timestamp to %d", timestamp);
            //}
            byte[] data = Bytes.toBytes(timestamp);
            rzk.setData(node, data, -1 /* version */);
            timestampSource.refresh();
        }
        else {
            //if (LOG.isDebugEnabled()) {
            SpliceLogUtils.info(LOG, "current timestamp = %d >  %d",
                    currentTimestamp, timestamp);
            //}
        }
    }

    public static Configuration createConfiguration(String clusterKey) {
        Configuration conf = new Configuration();
        String[] s = clusterKey.split(":");
        String quorum = s[0];
        int port = new Integer(s[1]);
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
            ResultSet rs = connection.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
            rs.next();
            return rs.getLong(1);
        }
    }

    public static Connection connect(Configuration conf, String clusterKey) throws IOException, InterruptedException, KeeperException, SQLException{
        List<String> regionServers = getRegionServers(conf);
        for (String rs : regionServers) {
            try {
                Connection connection = connectToRegionServer(rs);
                return connection;
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
                               boolean enabled,
                               boolean serial) throws SQLException, IOException, KeeperException, InterruptedException {
        String sql = String.format("call syscs_util.add_peer(%s, '%s', '%s', '%s')", peerId, peerClusterKey,
                enabled ? "true" : "false", serial ? "true" : "false");
        ResultSet rs = connection.createStatement().executeQuery(sql);
        rs.next();
        try {
            int index = rs.findColumn("Success");
            String msg = rs.getString(index);
            SpliceLogUtils.info(LOG, msg);
        }
        catch (SQLException e) {
            SpliceLogUtils.error(LOG, "Failed to add a peer: %s : ", peerClusterKey, rs.getString(1));
            throw e;
        }
    }

    public static void setupReplicationMaster(Connection connection, String clusterKey) throws SQLException {
        String sql = String.format("call syscs_util.set_replication_role('MASTER')");
        ResultSet rs = connection.createStatement().executeQuery(sql);
        rs.next();
        try {
            int index = rs.findColumn("Success");
            String msg = rs.getString(index);
            SpliceLogUtils.info(LOG, msg);
        }
        catch (SQLException e) {
            SpliceLogUtils.error(LOG, "Failed to setup master %s : ", clusterKey, rs.getString(1));
            throw e;
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
}
