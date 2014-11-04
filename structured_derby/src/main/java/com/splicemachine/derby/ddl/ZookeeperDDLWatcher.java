package com.splicemachine.derby.ddl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.ZkUtils;
import com.splicemachine.utils.kryo.KryoPool;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.*;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.exception.Exceptions;
import static com.splicemachine.derby.ddl.DDLZookeeperClient.*;

/**
 * An instance of this class in each region server listens for DDL notifications.
 */
public class ZookeeperDDLWatcher implements DDLWatcher, Watcher {

    private static final Logger LOG = Logger.getLogger(ZookeeperDDLWatcher.class);

    private static final long REFRESH_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(20);
    /*
     * We wait longer than the max DDL wait to make sure that we don't time out until after the controller
     * has
     */
    private static final long MAXIMUM_DDL_WAIT_MS = TimeUnit.SECONDS.toMillis(2*SpliceConstants.maxDdlWait);

    private Set<String> seenDDLChanges = new HashSet<String>();
    private Map<String, Long> changesTimeouts = new HashMap<String, Long>();
    private Map<String, DDLChange> currentDDLChanges = new HashMap<String, DDLChange>();
    private Map<String, DDLChange> tentativeDDLs = new ConcurrentHashMap<String, DDLChange>();

    private String id;

    private Set<DDLListener> ddlListeners = new CopyOnWriteArraySet<DDLListener>();

    private ExecutorService refreshThread = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("ZooKeeperDDLWatcherRefresher").setDaemon(true).build());

    private final Lock refreshNotifierLock = new ReentrantLock();
    private final Condition refreshNotifierCondition = refreshNotifierLock.newCondition();
    private final AtomicInteger requestCount = new AtomicInteger(0);


    @Override
    public void start() throws StandardException {
        createRequiredZooNodes();

        this.id = registerThisServer();
        if(id.startsWith("/"))
            id = id.substring(1); //strip the leading / to make sure that we register properly

        // run refresh() synchronously the first time
        refresh();

        refreshThread.submit(new Runnable() {
            @Override
            public void run() {
                /*
                 * We loop infinitely here, and rely on daemon threads to allow
                 * us to shut down properly. This way, we will just always be running
                 * as long as we are up.
                 */
                //noinspection InfiniteLoopStatement
                while(true){
                    int signalledWhileRefresh;
                    int currentSignalSize = requestCount.get();
                    try{
                        refresh();
                    }catch(Throwable e){
                        LOG.error("Failed to refresh ddl",e);
                    }

                    refreshNotifierLock.lock();
                    try{
                        signalledWhileRefresh = requestCount.addAndGet(-currentSignalSize);
                        //someone notified us while we were refreshing, so don't go to sleep yet
                        if(signalledWhileRefresh!=0)
                            continue;
                        /*
                         * Wait to be notified, but only up to the refresh interval. After that,
                         * we go ahead and refresh anyway.
                         */
                        refreshNotifierCondition.await(REFRESH_TIMEOUT_MS,TimeUnit.MILLISECONDS);
                    }catch (InterruptedException e) {
                        LOG.error("Interrupted while forcibly refreshing, terminating thread");
                    } finally{
                        refreshNotifierLock.unlock();
                    }
                }
            }
        });
    }

    private void signalRefresh() {
        /*
         * We use condition signalling to notify events; this is
         * a pretty decent way of ensuring that we don't waste resources
         * when nothing is going on. However, it is possible that, if you
         * receive a notification of an event while you are in the process
         * of refreshing, that that event may not be picked up. To prevent
         * this, we keep track of how many signals we emit, and the refresh
         * thread makes sure to perform refreshes if it received an event
         * while it wasn't listening.
         */
        refreshNotifierLock.lock();
        try {
            requestCount.incrementAndGet();
            refreshNotifierCondition.signal();
        }finally{
            refreshNotifierLock.unlock();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType().equals(EventType.NodeChildrenChanged)) {
            if(LOG.isTraceEnabled())
                LOG.trace("Received watch event, signalling refresh");
            signalRefresh();
        }
    }

    @Override
    public Set<DDLChange> getTentativeDDLs() {
        return new HashSet<DDLChange>(tentativeDDLs.values());
    }

    @Override
    public void registerDDLListener(DDLListener listener) {
        if(!currentDDLChanges.isEmpty())
            listener.startGlobalChange();
        this.ddlListeners.add(listener);
    }

    @Override
    public void unregisterDDLListener(DDLListener listener) {
        ddlListeners.remove(listener);
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private synchronized void refresh() throws StandardException {

        // Get all ongoing DDL changes
        List<String> ongoingDDLChangeIDs = getOngoingDDLChangeIDs(this);
        boolean currentWasEmpty = currentDDLChanges.isEmpty();
        Set<DDLChange> newChanges = new HashSet<DDLChange>();

        // remove finished ddl changes
        clearFinishedChanges(ongoingDDLChangeIDs);

        for (Iterator<String> iterator = ongoingDDLChangeIDs.iterator(); iterator.hasNext();) {
            String changeId = iterator.next();
            if (!seenDDLChanges.contains(changeId)) {
                byte[] data;
                try {
                    data = ZkUtils.getData(SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/" + changeId);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }

                DDLChange ddlChange = decode(data);
                LOG.debug("New change with id " + changeId + " :" + ddlChange);
                ddlChange.setChangeId(changeId);
                newChanges.add(ddlChange);
                seenDDLChanges.add(changeId);
                if (ddlChange.isTentative()) {
                    // isTentative means we never add to currentDDLChanges or timeouts.  In fact all we do
                    // is add the DDLChange to tentativeDDLs and make that collection available to others
                    // on this JVM.
                    processTentativeDDLChange(changeId, ddlChange);
                } else {
                    currentDDLChanges.put(changeId, ddlChange);
                    changesTimeouts.put(changeId, System.currentTimeMillis());
                    // notify access manager
                    for(DDLListener listener:ddlListeners){
                        listener.startChange(ddlChange);
                    }
                }
            }
        }

        //
        // CASE 1: currentDDLChanges was empty and we added changes.
        //  OR
        // CASE 2: currentDDLChanges was NOT empty and we removed everything.
        //
        if (currentWasEmpty != currentDDLChanges.isEmpty()) {
            boolean case1 = !currentDDLChanges.isEmpty();
            for(DDLListener listener:ddlListeners){
                if(case1){
                    listener.startGlobalChange();
                }else
                    listener.finishGlobalChange();
            }
        }

        // notify ddl operation we processed the change
        notifyProcessed(newChanges);
        killTimeouts();
    }

    private void clearFinishedChanges(List<String> children) {
        /*
         * Remove DDL changes which are known to be finished.
         *
         * This is to avoid processing a DDL change twice.
         */
        for (Iterator<String> iterator = seenDDLChanges.iterator(); iterator.hasNext();) {
            String entry = iterator.next();
            if (!children.contains(entry)) {
                LOG.debug("Removing change with id " + entry);
                changesTimeouts.remove(entry);
                currentDDLChanges.remove(entry);
                tentativeDDLs.remove(entry);
                iterator.remove();
                // notify access manager
                for(DDLListener listener:ddlListeners){
                    listener.finishChange(entry);
                }
            }
        }
    }

    private void killTimeouts() {
        /*
         * Kill transactions which have been timed out.
         */
        for (Entry<String, Long> entry : changesTimeouts.entrySet()) {
            if (System.currentTimeMillis() > entry.getValue() + MAXIMUM_DDL_WAIT_MS) {
                killDDLTransaction(entry.getKey());
            }
        }
    }

    private void notifyProcessed(Set<DDLChange> processedChanges) throws StandardException {
        /*
         * Notify the relevant controllers that their change has been processed
         */
        List<Op> ops = Lists.newArrayListWithExpectedSize(processedChanges.size());
        List<DDLChange> changeList = Lists.newArrayList();
        for (DDLChange change : processedChanges) {
            Op op = Op.create(SpliceConstants.zkSpliceDDLOngoingTransactionsPath+"/"+change.getChangeId()+"/"+id,new byte[]{},ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
            ops.add(op);
            changeList.add(change);
        }
        try {
            List<OpResult> multi = ZkUtils.getRecoverableZooKeeper().getZooKeeper().multi(ops);
            for(int i=0;i<multi.size();i++){
                OpResult result = multi.get(i);
                if(!(result instanceof OpResult.ErrorResult))
                    processedChanges.remove(changeList.get(i));
                else{
                    OpResult.ErrorResult err = (OpResult.ErrorResult)result;
                    Code code = Code.get(err.getErr());
                    switch(code){
                        case NODEEXISTS: //we may have already set the value, so ignore node exists issues
                        case NONODE: // someone already removed the notification, it's obsolete
                            // ignore
                            break;
                        default:
                            throw Exceptions.parseException(KeeperException.create(code));
                    }
                }
            }
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        } catch (KeeperException e) {
            switch(e.code()) {
                case NODEEXISTS: //we may have already set the value, so ignore node exists issues
                case NONODE: // someone already removed the notification, it's obsolete
                    // ignore
                    break;
                default:
                    throw Exceptions.parseException(e);
            }
        }
    }

    private DDLChange decode(byte[] data) {
        DDLChange ddlChange;
        KryoPool kp = SpliceKryoRegistry.getInstance();
        Kryo kryo = kp.get();

        try{
            Input input = new Input(data);
            ddlChange = kryo.readObject(input, DDLChange.class);
        }finally{
            kp.returnInstance(kryo);
        }
        return ddlChange;
    }

    private void processTentativeDDLChange(String changeId, DDLChange ddlChange) throws StandardException {
        switch (ddlChange.getChangeType()) {
            case CREATE_INDEX:
            case DROP_COLUMN:
            case ADD_COLUMN:
            case DROP_INDEX:
                tentativeDDLs.put(changeId, ddlChange);
                break;
            case DROP_TABLE:
            case DROP_SCHEMA:
                break;
            default:
                throw StandardException.newException(SQLState.UNSUPPORTED_TYPE);
        }
        for(DDLListener listener:ddlListeners){
            listener.startChange(ddlChange);
        }
    }

    private void killDDLTransaction(String changeId) {
        deleteChangeNode(changeId);
    }
}
