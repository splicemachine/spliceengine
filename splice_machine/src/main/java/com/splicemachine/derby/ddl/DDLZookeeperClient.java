package com.splicemachine.derby.ddl;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.hbase.SpliceBaseDerbyCoprocessor;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.ZkUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Zookeeper logic used by our ZookeeperDDLWatcher. Just hides some of the ugly zoo exception handling and lets
 * ZookeeperDDLWatcher focus on the main business logic.
 *
 */
class DDLZookeeperClient {

    private static final Logger LOG = Logger.getLogger(DDLZookeeperClient.class);

    private static final String SERVERS_PATH = SpliceConstants.zkSpliceDDLActiveServersPath;
    private static final String CHANGES_PATH = SpliceConstants.zkSpliceDDLOngoingTransactionsPath;
    private static final String DDL_PATH = SpliceConstants.zkSpliceDDLPath;

    static void createRequiredZooNodes() throws IOException {
        for (String path : new String[]{DDL_PATH, CHANGES_PATH, SERVERS_PATH}) {
            try {
                ZkUtils.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException e) {
                if (!e.code().equals(KeeperException.Code.NODEEXISTS)) {
                    throw new IOException(e);
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - -
    // methods for dealing with the server nodes
    // - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Returns this server id. If this isn't a server, returns null
     * @return null if not registered
     */
    static String registerThisServer() {
        /*
         * See DB-1812: Instead of creating our own server registration, we merely fetch our own
         * label from the RegionServers list which hbase maintains for us.
         */
        return SpliceBaseDerbyCoprocessor.regionServerZNode;
    }

    static Collection<String> getActiveServers(Watcher watcher) throws StandardException {
        try {
            return ZkUtils.getChildren(SpliceBaseDerbyCoprocessor.rsZnode, watcher);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - -
    // methods for dealing with the change nodes
    // - - - - - - - - - - - - - - - - - - - - - - -

    static String createChangeNode(byte[] changeData) throws StandardException {
        try {
            String changeId = ZkUtils.create(CHANGES_PATH + "/", changeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            return changeId.substring(changeId.lastIndexOf('/') + 1);
        } catch (KeeperException | InterruptedException e) {
            throw Exceptions.parseException(e);
        }
    }

    static void deleteChangeNode(String changeId) {
        try {
            ZkUtils.recursiveDelete(CHANGES_PATH + "/" + changeId);
        } catch (Exception e) {
            LOG.error("Couldn't delete change zookeeper node for DDL changeId=" + changeId, e);
        }
    }

    static List<String> getFinishedServers(String changeId, Watcher watcher) throws StandardException {
        try {
            return ZkUtils.getChildren(CHANGES_PATH + "/" + changeId, watcher);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    static String getServerChangeData(String changeId,String serverId) throws StandardException {
        try {
            return Bytes.toString(ZkUtils.getData(CHANGES_PATH + "/" + changeId + "/" + serverId));
        }
        catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

}