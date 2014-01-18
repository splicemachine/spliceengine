package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

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

import com.google.gson.Gson;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.ZkUtils;

public class ZookeeperDDLController implements DDLController, Watcher {
    private static final Logger LOG = Logger.getLogger(ZookeeperDDLController.class);

    private final Object lock = new Object();
    private static final long REFRESH_TIMEOUT = 2000; // timeout to refresh the info, in case some server is dead or a new server came up
    private static final long MAXIMUM_WAIT = 60000; // maximum wait for everybody to respond, after this we fail the DDL change
    private static Gson gson = new Gson();

    @Override
    public String notifyMetadataChange(DDLChange change) throws StandardException {
        String changeId;
        String jsonChange = gson.toJson(change);
        try {
            changeId = ZkUtils.create(SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/", Bytes.toBytes(jsonChange),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (KeeperException e) {
            throw Exceptions.parseException(e);
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        }

        long startTimestamp = System.currentTimeMillis();
        synchronized (lock) {
            while (true) {
                Collection<String> servers;
                try {
                    servers = ZkUtils.getChildren(SpliceConstants.zkSpliceDDLActiveServersPath, this);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
                List<String> children;
                try {
                    children = ZkUtils.getChildren(changeId, this);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
                boolean missing = false;
                for (String server : servers) {
                    if (!children.contains(server)) {
                        missing = true;
                        break;
                    }
                }


                if (!missing) {
                    // everybody responded, leave loop
                    break;
                }

                
                if (System.currentTimeMillis() - startTimestamp > MAXIMUM_WAIT) {
                    LOG.error("Maximum wait for all servers exceeded. Waiting response from " + servers
                            + ". Received response from " + children);
                    try {
                        ZkUtils.recursiveDelete(changeId);
                    } catch (Exception e) {
                        LOG.error("Couldn't kill transaction " + change.getTransactionId() + " for DDL change "
                                + change.getType() + " with id "+ changeId, e);
                    }
                    throw StandardException.newException(SQLState.LOCK_TIMEOUT,  "Wait of "
                            + (System.currentTimeMillis() - startTimestamp) + " exceeded timeout of " + MAXIMUM_WAIT);
                }
                try {
                    lock.wait(REFRESH_TIMEOUT);
                } catch (InterruptedException e) {
                    throw Exceptions.parseException(e);
                }
            }
        }
        return changeId;
    }

    @Override
    public void finishMetadataChange(String identifier) throws StandardException {
        try {
        	if (identifier != null && !identifier.startsWith("/")) {
                ZkUtils.recursiveDelete(SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/" + identifier);
        	} else {
                ZkUtils.recursiveDelete(identifier);        		
        	}
        } catch (Exception e) {
//            LOG.warn("Couldn't remove DDL change " + identifier, e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType().equals(EventType.NodeChildrenChanged)) {
            synchronized (lock) {
                lock.notify();
            }
        }
    }
}
