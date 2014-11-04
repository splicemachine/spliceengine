package com.splicemachine.derby.ddl;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDerbyCoprocessor;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.ZkUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;

/**
 * Zookeeper logic used by our ZookeeperDDLWatcher. Just hides some of the ugly zoo exception handling and lets
 * ZookeeperDDLWatcher focus on the main business logic.
 *
 * <pre>
 *
 * /ddl/activeServers
 *    server-0001
 *    server-0002
 *
 * /ddl/ongoingChanges
 *    change-0001/
 *    change-0002/       -- JSON encoded DDL Change
 *    change-0003/
 *        server-0001   -- Servers that have acknowledged the change.
 *        server-0002
 *
 * </pre>
 */
class DDLZookeeperClient {

    private static final Logger LOG = Logger.getLogger(DDLZookeeperClient.class);

    private static final String SERVERS_PATH = SpliceConstants.zkSpliceDDLActiveServersPath;
    private static final String CHANGES_PATH = SpliceConstants.zkSpliceDDLOngoingTransactionsPath;
    private static final String DDL_PATH = SpliceConstants.zkSpliceDDLPath;

    static void createRequiredZooNodes() throws StandardException {
        for (String path : new String[]{DDL_PATH, CHANGES_PATH, SERVERS_PATH}) {
            try {
                ZkUtils.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException e) {
                if (!e.code().equals(KeeperException.Code.NODEEXISTS)) {
                    throw Exceptions.parseException(e);
                }
            } catch (InterruptedException e) {
                throw Exceptions.parseException(e);
            }
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - -
    // methods for dealing with the server nodes
    // - - - - - - - - - - - - - - - - - - - - - - -

    static String registerThisServer() throws StandardException {
        /*
         * See DB-1812: Instead of creating our own server registration, we merely fetch our own
         * label from the RegionServers list which hbase maintains for us.
         */
        return SpliceDerbyCoprocessor.regionServerZNode;
    }

    static Collection<String> getActiveServers(Watcher watcher) throws StandardException {
        try {
            return ZkUtils.getChildren(SpliceDerbyCoprocessor.rsZnode, watcher);
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
        } catch (KeeperException e) {
            throw Exceptions.parseException(e);
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        }
    }

    static List<String> getOngoingDDLChangeIDs(Watcher watcher) throws StandardException {
        try {
            return ZkUtils.getChildren(CHANGES_PATH, watcher);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    static DDLChange getOngoingDDLChange(String changeId) throws StandardException {
        try {
            byte[] data = ZkUtils.getData(CHANGES_PATH + "/" + changeId);
            String jsonChange = Bytes.toString(data);
            DDLChange ddlChange = DDLCoordinationFactory.GSON.fromJson(jsonChange, DDLChange.class);
            ddlChange.setChangeId(changeId);
            return ddlChange;
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    static void acknowledgeChange(String changeId, String serverId) throws StandardException {
        try {
            ZkUtils.create(CHANGES_PATH + "/" + changeId + "/" + serverId, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException e) {
            switch (e.code()) {
                case NODEEXISTS: //we may have already set the value, so ignore node exists issues
                case NONODE: // someone already removed the notification, it's obsolete
                    // ignore
                    break;
                default:
                    throw Exceptions.parseException(e);
            }
        } catch (InterruptedException e) {
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

}