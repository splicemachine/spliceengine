package com.splicemachine.derby.ddl;

import com.google.common.collect.Lists;
import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.splicemachine.derby.ddl.ZookeeperDDLWatcherClient.*;

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
    private ScheduledExecutorService executor = MoreExecutors
            .namedSingleThreadScheduledExecutor("ZookeeperDDLWatcherRefresh");

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

        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    refresh();
                } catch (StandardException e) {
                    LOG.error("Failed to execute refresh", e);
                }
            }
        }, REFRESH_TIMEOUT_MS, REFRESH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType().equals(EventType.NodeChildrenChanged)) {
            try {
                refresh();
            } catch (StandardException e) {
                LOG.error("Couldn't process the ZooKeeper event " + event, e);
            }
        }
    }

    @Override
    public Set<DDLChange> getTentativeDDLs() {
        return new HashSet<DDLChange>(tentativeDDLs.values());
    }

    private synchronized void refresh() throws StandardException {
        initializeAccessManager();

        // Get all ongoing DDL changes
        List<String> ongoingDDLChangeIDs = getOngoingDDLChangeIDs(this);
        boolean currentWasEmpty = currentDDLChanges.isEmpty();
        Set<DDLChange> newChanges = new HashSet<DDLChange>();

        //
        // Remove form all records changeIds that are not currently ongoing.
        //
        for (Iterator<String> iterator = seenDDLChanges.iterator(); iterator.hasNext(); ) {
            String seenChangeId = iterator.next();
            if (!ongoingDDLChangeIDs.contains(seenChangeId)) {
                LOG.debug("Removing change with id " + seenChangeId);
                changesTimeouts.remove(seenChangeId);
                currentDDLChanges.remove(seenChangeId);
                tentativeDDLs.remove(seenChangeId);
                iterator.remove();
                // notify access manager
                if (accessManager != null) {
                    accessManager.finishDDLChange(seenChangeId);
                }
            }
        }

        //
        // Process NEW changeIds that we have not seen.
        //
        for (String changeId : ongoingDDLChangeIDs) {
            if (!seenDDLChanges.contains(changeId)) {
                DDLChange ddlChange = getOngoingDDLChange(changeId);
                newChanges.add(ddlChange);
                LOG.debug("New change with id " + changeId + " :" + ddlChange);
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

        //
        // NEW changes: notify ddl controller we processed the change
        //
        for (DDLChange ddlChange : newChanges) {
            acknowledgeChange(ddlChange.getChangeId(), id);
        }

        //
        // Kill DDL transactions that have been ongoing for more than our timeout value.
        //
        for (Entry<String, Long> entry : changesTimeouts.entrySet()) {
            if (System.currentTimeMillis() > entry.getValue() + MAXIMUM_DDL_WAIT_MS) {
                killDDLTransaction(entry.getKey());
            }
        }
    }

    private void processTentativeDDLChange(String changeId, DDLChange ddlChange) throws StandardException {
        switch (ddlChange.getChangeType()) {
            case CREATE_INDEX:
            case DROP_COLUMN:
                tentativeDDLs.put(changeId, ddlChange);
                break;
            default:
                throw StandardException.newException(SQLState.UNSUPPORTED_TYPE);
        }
    }

    private void killDDLTransaction(String changeId) {
        try {
            String transactionId = currentDDLChanges.get(changeId).getTransactionId();
            LOG.warn("We are killing transaction " + transactionId + " since it exceeds the maximum wait period for"
                    + " the DDL change " + changeId + " publication");
            HTransactorFactory.getTransactionManager().fail(new TransactionId(transactionId));
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
        Collection<LanguageConnectionContext> result = Lists.newArrayList();
        List<Context> allContexts = ContextService.getFactory().getAllContexts(LanguageConnectionContext.CONTEXT_ID);
        for (Context c : allContexts) {
            result.add((LanguageConnectionContext) c);
        }
        return result;
    }
}
