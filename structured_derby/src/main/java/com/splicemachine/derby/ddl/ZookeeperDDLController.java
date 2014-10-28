package com.splicemachine.derby.ddl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.exception.Exceptions;
import static com.splicemachine.derby.ddl.DDLZookeeperClient.*;

/**
 * Used on the node where DDL changes are initiated to communicate changes to other nodes.
 */
public class ZookeeperDDLController implements DDLController, Watcher {

    private static final Logger LOG = Logger.getLogger(ZookeeperDDLController.class);

    private final Lock notificationLock = new ReentrantLock();
    private final Condition notificationSignal = notificationLock.newCondition();

    // timeout to refresh the info, in case some server is dead or a new server came up
    private static final long REFRESH_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
    // maximum wait for everybody to respond, after this we fail the DDL change
    private static final long MAXIMUM_WAIT = TimeUnit.SECONDS.toMillis(SpliceConstants.maxDdlWait);

    private final ActiveServerList activeServers = new ActiveServerList();
    @Override
    public String notifyMetadataChange(DDLChange change) throws StandardException {
        byte[] data = encode(change);
        String changeId = DDLZookeeperClient.createChangeNode(data);

        long availableTime = MAXIMUM_WAIT;
        long elapsedTime = 0;
        while (true) {
            Collection<String> activeServers = this.activeServers.getActiveServers();
            Collection<String> finishedServers = getFinishedServers(changeId, this);

            if (finishedServers.containsAll(activeServers)) {
                // everybody responded, leave loop
                break;
            }

            if (availableTime<0) {
                LOG.error("Maximum wait for all servers exceeded. Waiting response from " + activeServers
                        + ". Received response from " + finishedServers);
                deleteChangeNode(changeId);
                throw ErrorState.DDL_TIMEOUT.newException(elapsedTime,MAXIMUM_WAIT);
            }
            long startTimestamp = System.currentTimeMillis();
            notificationLock.lock();
            try {
                notificationSignal.await(REFRESH_TIMEOUT,TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw Exceptions.parseException(e);
            }finally{
                notificationLock.unlock();
            }
            long stopTimestamp = System.currentTimeMillis();
            availableTime-= (stopTimestamp -startTimestamp);
            elapsedTime+=(stopTimestamp-startTimestamp);
        }
        return changeId;
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
            notificationLock.lock();
            try{
                notificationSignal.signalAll();
            }finally{
                notificationLock.unlock();
            }
        }
    }

    private static class ActiveServerList implements Watcher{
        private volatile boolean needsRefreshed = true;
        private volatile Collection<String> activeServers;

        public Collection<String> getActiveServers() throws StandardException {
            if(needsRefreshed){
                synchronized (this){
                    if(needsRefreshed){
                        activeServers = DDLZookeeperClient.getActiveServers(this);
                        needsRefreshed = false;
                    }
                }
            }
            return activeServers;
        }

        @Override
        public void process(WatchedEvent watchedEvent) {
            needsRefreshed = true;
        }
    }

}
