package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.ZkUtils;

public class ZookeeperDDLController implements DDLController, Watcher {

    private static final Logger LOG = Logger.getLogger(ZookeeperDDLController.class);

    private final Object LOCK = new Object();

    // timeout to refresh the info, in case some server is dead or a new server came up
    private static final long REFRESH_TIMEOUT = TimeUnit.SECONDS.toMicros(2);
    // maximum wait for everybody to respond, after this we fail the DDL change
    private static final long MAXIMUM_WAIT = TimeUnit.SECONDS.toMillis(60);

    @Override
    public String notifyMetadataChange(DDLChange change) throws StandardException {

        String jsonChange = DDLCoordinationFactory.GSON.toJson(change);
        String changeId = createZkNode(jsonChange);

        LOG.debug("Notifying metadata change with id " + changeId + ": " + jsonChange);

        long startTimestamp = System.currentTimeMillis();
        synchronized (LOCK) {
            while (true) {
                Collection<String> activeServers = getActiveServers();
                Collection<String> finishedServers = getFinishedServers(changeId);

                if (finishedServers.containsAll(activeServers)) {
                    // everybody responded, leave loop
                    break;
                }

                long elapsedTime = System.currentTimeMillis() - startTimestamp;

                if (elapsedTime > MAXIMUM_WAIT) {
                    LOG.error("Maximum wait for all servers exceeded. Waiting response from " + activeServers
                            + ". Received response from " + finishedServers);
                    try {
                        ZkUtils.recursiveDelete(changeId);
                    } catch (Exception e) {
                        LOG.error("Couldn't kill transaction " + change.getTransactionId() + " for DDL change "
                                + change.getType() + " with id " + changeId, e);
                    }
                    throw StandardException.newException(SQLState.LOCK_TIMEOUT, "Wait of "
                            + elapsedTime + " exceeded timeout of " + MAXIMUM_WAIT);
                }
                try {
                    LOCK.wait(REFRESH_TIMEOUT);
                } catch (InterruptedException e) {
                    throw Exceptions.parseException(e);
                }
            }
        }
        return changeId;
    }

    @Override
    public void finishMetadataChange(String identifier) throws StandardException {
        LOG.debug("Finishing metadata change with id " + identifier);
        try {
            if (identifier != null && !identifier.startsWith("/")) {
                ZkUtils.recursiveDelete(SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/" + identifier);
            } else {
                ZkUtils.recursiveDelete(identifier);
            }
        } catch (Exception e) {
            LOG.warn("Couldn't remove DDL change " + identifier, e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType().equals(EventType.NodeChildrenChanged)) {
            synchronized (LOCK) {
                LOCK.notify();
            }
        }
    }

    private List<String> getFinishedServers(String changeId) throws StandardException {
        try {
            return ZkUtils.getChildren(changeId, this);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    private Collection<String> getActiveServers() throws StandardException {
        try {
            return ZkUtils.getChildren(SpliceConstants.zkSpliceDDLActiveServersPath, this);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    private static String createZkNode(String jsonChange) throws StandardException {
        try {
            String path = SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/";
            return ZkUtils.create(path, Bytes.toBytes(jsonChange), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (KeeperException e) {
            throw Exceptions.parseException(e);
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        }
    }
}
