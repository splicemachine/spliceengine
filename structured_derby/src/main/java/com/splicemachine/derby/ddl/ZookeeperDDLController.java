package com.splicemachine.derby.ddl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.ZkUtils;
import com.splicemachine.utils.kryo.KryoPool;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static com.splicemachine.derby.ddl.DDLZookeeperClient.*;

/**
 * Used on the node where DDL changes are initiated to communicate changes to other nodes.
 */
public class ZookeeperDDLController implements DDLController, Watcher {

    private static final Logger LOG = Logger.getLogger(ZookeeperDDLController.class);

    private final Object LOCK = new Object();

    // timeout to refresh the info, in case some server is dead or a new server came up
    private static final long REFRESH_TIMEOUT = TimeUnit.SECONDS.toMillis(2);
    // maximum wait for everybody to respond, after this we fail the DDL change
    private static final long MAXIMUM_WAIT = TimeUnit.SECONDS.toMillis(60);

    @Override
    public String notifyMetadataChange(DDLChange change) throws StandardException {
        byte[] data = encode(change);
        String changeId;
        try {
            if(LOG.isTraceEnabled())
                LOG.trace("Creating DDL event");
            changeId = ZkUtils.create(SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/", data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (KeeperException e) {
            throw Exceptions.parseException(e);
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        }

        if(LOG.isDebugEnabled())
            LOG.debug("Notifying metadata change with id " + changeId + ": " + jsonChange);

        long startTimestamp = System.currentTimeMillis();
        synchronized (LOCK) {
            while (true) {
                Collection<String> activeServers = getActiveServers(this);
                Collection<String> finishedServers = getFinishedServers(changeId, this);

                if (finishedServers.containsAll(activeServers)) {
                    // everybody responded, leave loop
                    break;
                }

                long elapsedTime = System.currentTimeMillis() - startTimestamp;

                if (elapsedTime > MAXIMUM_WAIT) {
                    LOG.error("Maximum wait for all servers exceeded. Waiting response from " + activeServers
                            + ". Received response from " + finishedServers);
                    deleteChangeNode(changeId);
                    throw StandardException.newException(SQLState.LOCK_TIMEOUT, "Wait of " + elapsedTime +
                            " exceeded timeout of " + MAXIMUM_WAIT);
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

    private List<String> getCompletedServers(String changeId) throws StandardException {
        /*
         * Gets all the servers that have actively completed this change
         */
        List<String> children;
        try {
            children = ZkUtils.getChildren(changeId, this);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        return children;
    }

    private Collection<String> getAllServers() throws StandardException {
        /*
         * Get all the servers that are actively a part of this cluster
         */
        Collection<String> servers;
        try {
            servers = ZkUtils.getChildren(SpliceConstants.zkSpliceDDLActiveServersPath, this);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        return servers;
    }

    protected byte[] encode(DDLChange change) {
        KryoPool kp = SpliceKryoRegistry.getInstance();
        Kryo kryo = kp.get();
        byte[] data;
        try{
            Output output = new Output(128,-1);
            kryo.writeObject(output,change);
            data = output.toBytes();
        }finally{
           kp.returnInstance(kryo);
        }
        return data;
    }

    @Override
    public void finishMetadataChange(String changeId) throws StandardException {
        LOG.debug("Finishing metadata change with id " + changeId);
        deleteChangeNode(changeId);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType().equals(EventType.NodeChildrenChanged)) {
            synchronized (LOCK) {
                LOCK.notify();
            }
        }
    }

}
