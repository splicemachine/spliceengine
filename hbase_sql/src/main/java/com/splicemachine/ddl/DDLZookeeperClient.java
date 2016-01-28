package com.splicemachine.ddl;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.hbase.RegionServerLifecycleObserver;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
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

    final String changePath;

    public DDLZookeeperClient(SConfiguration config){
        String rootPath = config.getString(HConfiguration.SPLICE_ROOT_PATH);
        this.changePath = rootPath+HConfiguration.DDL_CHANGE_PATH;
    }


    // - - - - - - - - - - - - - - - - - - - - - - -
    // methods for dealing with the server nodes
    // - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Returns this server id. If this isn't a server, returns null
     * @return null if not registered
     */
    String registerThisServer() {
        /*
         * See DB-1812: Instead of creating our own server registration, we merely fetch our own
         * label from the RegionServers list which hbase maintains for us.
         */
        return RegionServerLifecycleObserver.regionServerZNode;
    }

    Collection<String> getActiveServers(Watcher watcher) throws StandardException {
        try {
            return ZkUtils.getChildren(RegionServerLifecycleObserver.rsZnode, watcher);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - -
    // methods for dealing with the change nodes
    // - - - - - - - - - - - - - - - - - - - - - - -

    String createChangeNode(byte[] changeData) throws StandardException {
        try {
            String changeId = ZkUtils.create(changePath+ "/", changeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            return changeId.substring(changeId.lastIndexOf('/') + 1);
        } catch (KeeperException | InterruptedException e) {
            throw Exceptions.parseException(e);
        }
    }

    void deleteChangeNode(String changeId) {
        try {
            ZkUtils.recursiveDelete(changePath+ "/" + changeId);
        } catch (Exception e) {
            LOG.error("Couldn't delete change zookeeper node for DDL changeId=" + changeId, e);
        }
    }

    List<String> getFinishedServers(String changeId, Watcher watcher) throws StandardException {
        try {
            return ZkUtils.getChildren(changePath+ "/" + changeId, watcher);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    String getServerChangeData(String changeId,String serverId) throws StandardException {
        try {
            return Bytes.toString(ZkUtils.getData(changePath+ "/" + changeId + "/" + serverId));
        }
        catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }
}