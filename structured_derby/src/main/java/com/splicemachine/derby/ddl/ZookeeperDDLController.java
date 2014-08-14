package com.splicemachine.derby.ddl;

import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.ZkUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.splicemachine.derby.ddl.ZookeeperDDLWatcherClient.*;

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

        String jsonChange = DDLCoordinationFactory.GSON.toJson(change);
        String changeId = createChangeNode(jsonChange);

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
