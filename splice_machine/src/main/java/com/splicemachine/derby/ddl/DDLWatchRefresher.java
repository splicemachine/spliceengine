package com.splicemachine.derby.ddl;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.TransactionReadController;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Date: 9/7/15
 */
public class DDLWatchRefresher{
    private static final Logger LOG=Logger.getLogger(DDLWatchRefresher.class);
    private final Set<String> seenDDLChanges;
    private final Map<String, Long> changeTimeouts;
    private final Map<String, DDLChange> currentDDLChanges;
    private final Map<String, DDLChange> tentativeDDLS;
    private AtomicReference<DDLFilter> ddlDemarcationPoint;
    private final DDLWatchChecker watchChecker;
    private final Clock clock;
    private final long maxDdlWaitMs;
    private final AtomicInteger currChangeCount= new AtomicInteger(0);


    public DDLWatchRefresher(DDLWatchChecker watchChecker,
                             Clock clock,long maxDdlWaitMs){
        this.clock=clock;
        this.maxDdlWaitMs=maxDdlWaitMs;
        this.seenDDLChanges=new HashSet<>();
        this.changeTimeouts=new HashMap<>();
        this.currentDDLChanges=new HashMap<>();
        this.tentativeDDLS=new ConcurrentHashMap<>();
        this.watchChecker=watchChecker;
        ddlDemarcationPoint = new AtomicReference<>();
    }

    public Collection<DDLChange> tentativeDDLChanges(){
        return tentativeDDLS.values();
    }

    public int numCurrentDDLChanges(){
        return currChangeCount.get();
    }

    public boolean refreshDDL(Set<DDLWatcher.DDLListener> callbacks) throws IOException, StandardException{
        List<String> ongoingDDLChangeIds=watchChecker.getCurrentChangeIds();
        if(ongoingDDLChangeIds==null) return false;

        Set<Pair<DDLChange,String>> newChanges=new HashSet<>();
        boolean currentWasEmpty=currentDDLChanges.isEmpty();

        clearFinishedChanges(ongoingDDLChangeIds,callbacks);

        for(String changeId : ongoingDDLChangeIds){
            if(!seenDDLChanges.contains(changeId)){
                DDLChange change=watchChecker.getChange(changeId);
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG,"New change with id=%s, and change=%s",changeId,change);
                try {
                    processPreCommitChange(change, callbacks);
                    seenDDLChanges.add(changeId);
                    newChanges.add(new Pair<DDLChange, String>(change,null));
                } catch (Exception e) {
                    if (LOG.isDebugEnabled())
                        SpliceLogUtils.error(LOG,e);
                    newChanges.add(new Pair<DDLChange, String>(change,e.getLocalizedMessage()));
                }
            }
        }

        watchChecker.notifyProcessed(newChanges);
        int killed = killTimeouts(callbacks);
        //
        // CASE 1: currentDDLChanges was empty and we added changes.
        //  OR
        // CASE 2: currentDDLChanges was NOT empty and we removed everything.
        //
        if(currentWasEmpty!=currentDDLChanges.isEmpty()){
            boolean case1=!currentDDLChanges.isEmpty();
            for(DDLWatcher.DDLListener listener : callbacks){
                if(case1){
                    listener.startGlobalChange();
                }else
                    listener.finishGlobalChange();
            }
        }else if(killed>0){
            /*
             * DB-3816. We need to re-enable caches if we killed some DDL
             * operations, because otherwise we may leak memory
             */
            for(DDLWatcher.DDLListener listener : callbacks){
                listener.finishGlobalChange();
            }
        }

        return true;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private void processPreCommitChange(DDLChange ddlChange,
                                        Collection<DDLWatcher.DDLListener> ddlListeners) throws StandardException {
        currChangeCount.incrementAndGet();
        tentativeDDLS.put(ddlChange.getChangeId(),ddlChange);
        for(DDLWatcher.DDLListener listener:ddlListeners){
            listener.startChange(ddlChange);
        }
    }

    private void clearFinishedChanges(List<String> children,Collection<DDLWatcher.DDLListener> ddlListeners) throws StandardException {
        /*
         * Remove DDL changes which are known to be finished.
         *
         * This is to avoid processing a DDL change twice.
         */
        for(Iterator<String> iterator=seenDDLChanges.iterator();iterator.hasNext();){
            String entry=iterator.next();
            if(!children.contains(entry)){
                LOG.debug("Removing change with id " + entry);
                changeTimeouts.remove(entry);
                currentDDLChanges.remove(entry);
                currChangeCount.decrementAndGet();
                DDLChange ddlChange = tentativeDDLS.remove(entry);
                assignDDLDemarcationPoint(ddlChange);
                iterator.remove();
                // notify access manager
                for(DDLWatcher.DDLListener listener : ddlListeners){
                    listener.changeSuccessful(entry,ddlChange);
                }
            }
        }
    }


    private void assignDDLDemarcationPoint(DDLChange ddlChange) {
        try {
            TxnView txn = DDLUtils.getLazyTransaction(ddlChange.getTxnId());
            assert txn.allowsWrites(): "DDLChange "+ddlChange+" does not have a writable transaction";
            TransactionReadController txController = HTransactorFactory.getTransactionReadController();
            DDLFilter ddlFilter = txController.newDDLFilter(txn);
            if (ddlFilter.compareTo(ddlDemarcationPoint.get()) > 0) {
                ddlDemarcationPoint.set(ddlFilter);
            }
        } catch (IOException e) {
            LOG.error("Couldn't create ddlFilter", e);
        }

    }

    private int killTimeouts(Set<DDLWatcher.DDLListener> listeners) {
        /*
         * Kill transactions which have been timed out.
         */
        int numKilled = 0;
        Iterator<Map.Entry<String,Long>> timeoutsIter = changeTimeouts.entrySet().iterator();
        while(timeoutsIter.hasNext()){
            Map.Entry<String,Long> entry = timeoutsIter.next();
            if (clock.currentTimeMillis() > entry.getValue() + maxDdlWaitMs) {
                watchChecker.killDDLTransaction(entry.getKey());
                /*
                 * DB-3816. We need to notify our listeners that we failed
                 * an operation, to avoid leaking resources due to never telling
                 * them to stop dealing with a specified change
                 */
                for(DDLWatcher.DDLListener listener:listeners){
                    listener.changeFailed(entry.getKey());
                }
                numKilled++;
                timeoutsIter.remove();
            }
        }
        return numKilled;
    }

    public boolean cacheIsValid() {
        return currChangeCount.get() ==0;
    }

    private boolean canSeeDDLDemarcationPoint(TransactionManager xact_mgr) {
        try {
            // If the transaction is older than the latest DDL operation (can't see it), bypass the cache
            DDLFilter filter = ddlDemarcationPoint.get();
            return filter == null || filter.isVisibleBy(((SpliceTransactionManager)xact_mgr).getActiveStateTxn());
        } catch (IOException e) {
            // Stay on the safe side, assume it's not visible
            return false;
        }
    }

    public boolean canUseCache(TransactionManager xact_mgr) {
        return cacheIsValid() && canSeeDDLDemarcationPoint(xact_mgr);
    }

}
