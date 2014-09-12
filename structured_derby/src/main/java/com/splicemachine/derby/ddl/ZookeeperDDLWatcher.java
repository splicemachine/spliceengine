package com.splicemachine.derby.ddl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.si.api.TransactionLifecycle;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ZkUtils;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.ShutdownException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.splicemachine.derby.ddl.DDLZookeeperClient.*;

/**
 * An instance of this class in each region server listens for DDL notifications.
 */
public class ZookeeperDDLWatcher implements DDLWatcher, Watcher {

    private static final Logger LOG = Logger.getLogger(ZookeeperDDLWatcher.class);

    private static final long REFRESH_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long MAXIMUM_DDL_WAIT_MS = TimeUnit.SECONDS.toMillis(90);

    private Set<String> seenDDLChanges = new HashSet<String>();
    private Map<String, Long> changesTimeouts = new HashMap<String, Long>();
    private Map<String, DDLChange> currentDDLChanges = new HashMap<String, DDLChange>();
    private Map<String, DDLChange> tentativeDDLs = new ConcurrentHashMap<String, DDLChange>();

    private String id;
    private SpliceAccessManager accessManager;

    private ExecutorService refreshThread = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("ZooKeeperDDLWatcherRefresher").setDaemon(true).build());

    private final Lock refreshNotifierLock = new ReentrantLock();
    private final Condition refreshNotifierCondition = refreshNotifierLock.newCondition();
    private final AtomicInteger requestCount = new AtomicInteger(0);

    @Override
    public synchronized void registerLanguageConnectionContext(LanguageConnectionContext lcc) {
        if (!currentDDLChanges.isEmpty()) {
            lcc.startGlobalDDLChange();
        }
    }

    @Override
    public void start() throws StandardException {
        createRequiredZooNodes();

        this.id = registerThisServer();

        // run refresh() synchronously the first time
        refresh();

//        executor.scheduleAtFixedRate(new Runnable() {
//            @Override
//            public void run() {
//
//                signalRefresh();
//            }
//        }, REFRESH_TIMEOUT, REFRESH_TIMEOUT, TimeUnit.MILLISECONDS);

        refreshThread.submit(new Runnable() {
            @Override
            public void run() {
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
                        //wait to be notified
                        refreshNotifierCondition.await();
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

    private void createZKTree() throws StandardException {
        for (String path : new String[] { SpliceConstants.zkSpliceDDLPath,
                SpliceConstants.zkSpliceDDLOngoingTransactionsPath, SpliceConstants.zkSpliceDDLActiveServersPath }) {
            try {
                ZkUtils.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException e) {
                if (e.code().equals(Code.NODEEXISTS)) {
                    // ignore
                } else {
                    throw Exceptions.parseException(e);
                }
            } catch (InterruptedException e) {
                throw Exceptions.parseException(e);
            }
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

    /*****************************************************************************************************************/
    /*private helper methods*/
    private synchronized void refresh() throws StandardException {
        initializeAccessManager();

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
                    if (accessManager != null) {
                        accessManager.startDDLChange(ddlChange);
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

            for (LanguageConnectionContext langContext : getLanguageConnectionContexts()) {

                // CASE 2: We are no longer aware of any ongoing DDL changes.
                if (currentDDLChanges.isEmpty()) {
                    LOG.debug("Finishing global ddl changes ");
                    // we can use caches again
                    langContext.finishGlobalDDLChange();
                }
                // CASE 1: DDL changes have started.
                else {
                    LOG.debug("Starting global ddl changes, invalidate and disable caches");
                    // we have to invalidate and disable caches
                    langContext.startGlobalDDLChange();
                }
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
                if (accessManager != null) {
                    accessManager.finishDDLChange(entry);
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
        for (DDLChange change : processedChanges) {
            try {
                ZkUtils.create(SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/" + change.getChangeId() + "/" + id,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                switch(e.code()) {
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
                tentativeDDLs.put(changeId, ddlChange);
                break;
            case DROP_TABLE:
                /* Clear DD caches on remote nodes for each DDL statement.  Before we did this remote nodes would
                 * correctly generate new activations classes and instances of constant action classes for statements on
                 * tables dropped and re-added with the same name, but would include in them stale information from the
                 * DD caches (conglomerate ID, for example) */
                for (LanguageConnectionContext lcc : getLanguageConnectionContexts()) {
                    lcc.getDataDictionary().clearCaches();
                }
                break;
            default:
                throw StandardException.newException(SQLState.UNSUPPORTED_TYPE);
        }
    }

    private void killDDLTransaction(String changeId) {
        try {
            TxnView txn = currentDDLChanges.get(changeId).getTxn();
            LOG.warn("We are killing transaction " + txn + " since it exceeds the maximum wait period for"
                    + " the DDL change " + changeId + " publication");
            TransactionLifecycle.getLifecycleManager().rollback(txn.getTxnId());
        } catch (Exception e) {
            LOG.warn("Couldn't kill transaction, already killed?", e);
        }
        deleteChangeNode(changeId);
    }


    private void initializeAccessManager() throws StandardException {
        if (accessManager != null) {
            // already initialized
            return;
        }
        if (Monitor.getMonitor() == null) {
            // can't initialize yet
            return;
        }
        SpliceDatabase db = ((SpliceDatabase) Monitor.findService(Property.DATABASE_MODULE, SpliceConstants.SPLICE_DB));
        if (db == null) {
            // can't initialize yet
            return;
        }
        accessManager = (SpliceAccessManager) Monitor.findServiceModule(db, AccessFactory.MODULE);
        for (DDLChange change : currentDDLChanges.values()) {
            accessManager.startDDLChange(change);
        }
    }

    private Collection<LanguageConnectionContext> getLanguageConnectionContexts() {
        try {
            return ContextService.getFactory().getAllContexts(LanguageConnectionContext.CONTEXT_ID);
        } catch (ShutdownException e) {
            LOG.warn("could not get contexts", e);
            /* Context service shutdown--return an empty list of contexts. */
            return Lists.newArrayList();
        }
    }
}
