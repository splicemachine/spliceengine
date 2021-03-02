/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.ddl;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.txn.LazyTxnView;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.splicemachine.ddl.DDLMessage.DDLChangeType.ENTER_RESTORE_MODE;

/**
 * @author Scott Fines
 *         Date: 9/7/15
 */
public class DDLWatchRefresher{
    private static final Logger LOG=Logger.getLogger(DDLWatchRefresher.class);
    private final Set<String> seenDDLChanges;
    private final Set<String> changeTimeouts;
    private final Map<String, DDLChange> currentDDLChanges;
    private final Map<String, DDLChange> tentativeDDLS;
    private final AtomicReference<DDLFilter> ddlDemarcationPoint;
    private final DDLWatchChecker watchChecker;
    private final TransactionReadController txController;
    private final AtomicInteger currChangeCount= new AtomicInteger(0);
    private final SqlExceptionFactory exceptionFactory;
    private final TxnSupplier txnSupplier;


    public DDLWatchRefresher(DDLWatchChecker watchChecker,
                             TransactionReadController txnController,
                             SqlExceptionFactory exceptionFactory,
                             TxnSupplier txnSupplier){
        this.txController = txnController;
        this.seenDDLChanges = ConcurrentHashMap.newKeySet();
        this.changeTimeouts = ConcurrentHashMap.newKeySet();
        this.currentDDLChanges = new ConcurrentHashMap<>();
        this.tentativeDDLS = new ConcurrentHashMap<>();
        this.watchChecker = watchChecker;
        this.exceptionFactory = exceptionFactory;
        this.txnSupplier = txnSupplier;
        ddlDemarcationPoint = new AtomicReference<>();
    }

    public Collection<DDLChange> tentativeDDLChanges(){
        return tentativeDDLS.values();
    }

    public int numCurrentDDLChanges(){
        return currChangeCount.get();
    }

    public boolean refreshDDL(Set<DDLWatcher.DDLListener> callbacks) throws IOException{
        Collection<String> ongoingDDLChangeIds=watchChecker.getCurrentChangeIds();
        if(ongoingDDLChangeIds==null) return false;

        Set<Pair<DDLChange,String>> newChanges=new HashSet<>();
        boolean currentWasEmpty=currentDDLChanges.isEmpty();

        try{
            clearFinishedChanges(ongoingDDLChangeIds,callbacks);
        }catch(StandardException se){
            throw exceptionFactory.asIOException(se);
        }

        for(String changeId : ongoingDDLChangeIds){
            if(!seenDDLChanges.contains(changeId)){
                DDLChange change=watchChecker.getChange(changeId);
                if(change==null)continue; //another thread took care of this for us

                //inform the server of the first time we see this change
                String cId=change.getChangeId();
                changeTimeouts.add(cId);
                SpliceLogUtils.info(LOG,"New change with id=%s, and change=%s",changeId,change);
                try {
                    processPreCommitChange(change, callbacks);
                    seenDDLChanges.add(changeId);
                    newChanges.add(new Pair<DDLChange, String>(change,null));
                } catch (Exception e) {
                    LOG.error("Encountered an exception processing DDL change",e);
                    newChanges.add(new Pair<>(change,e.getLocalizedMessage()));
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

    public boolean canUseSPSCache(TransactionManager txnMgr){
        /*
         * -sf- TODO resolve this more clearly
         *
         * The SPS Cache is a cache of stored prepared statements (SPS). When you initially
         * call an SPS, then it first attempts to read that information from tables, then it recompiles
         * and writes some stuff back to the database. After that, it is cached, which is totally cool. However,
         * if the cache is disabled, then the second time around the recompile phase may throw essentially
         * a Unique Constraint violation; as a result, when performing DDL operations, the SPS cache issue may
         * result in weird duplication errors. To temporarily bypass this, we make it so that you always
         * can use the SPS cache, but that is probably not the correct behavior in all cases. We'll find out
         * when it starts to smell.
         */
        return true;
    }

    public boolean canWriteCache(TransactionManager xact_mgr) {
        return cacheIsValid() && canSeeDDLDemarcationPoint(xact_mgr);
    }

    public boolean canReadCache(TransactionManager xact_mgr) {
        return canSeeDDLDemarcationPoint(xact_mgr);
    }


    /* ****************************************************************************************************************/
    /*private helper methods*/

    private void processPreCommitChange(DDLChange ddlChange,
                                        Collection<DDLWatcher.DDLListener> ddlListeners) throws StandardException {
        if (LOG.isTraceEnabled())
            LOG.trace("processPreCommitChanges -> " + ddlChange);
        currChangeCount.incrementAndGet();
        tentativeDDLS.put(ddlChange.getChangeId(),ddlChange);
        for(DDLWatcher.DDLListener listener:ddlListeners){
            listener.startChange(ddlChange);
        }
    }

    private void clearFinishedChanges(Collection<String> children,Collection<DDLWatcher.DDLListener> ddlListeners) throws StandardException {
        /*
         * Remove DDL changes which are known to be finished.
         *
         * This is to avoid processing a DDL change twice.
         *
         */
        for(Iterator<String> iterator=seenDDLChanges.iterator();iterator.hasNext();){
            String entry=iterator.next();
            if(!children.contains(entry)){
                LOG.info("Removing change with id " + entry);
                changeTimeouts.remove(entry);
                currentDDLChanges.remove(entry);
                currChangeCount.decrementAndGet();
                DDLChange ddlChange = tentativeDDLS.remove(entry);
                iterator.remove();
                if(ddlChange!=null){
                    /*
                     * If the change isn't in tentativeDDLs, then it's already been processed, and we don't
                     * have to worry about it here.
                     */
                    assignDDLDemarcationPoint(ddlChange);
                    // notify access manager
                    for(DDLWatcher.DDLListener listener : ddlListeners){
                        listener.changeSuccessful(entry,ddlChange);
                    }
                }
            }
        }
    }


    private void assignDDLDemarcationPoint(DDLChange ddlChange) {
        try {
            TxnView txn = new LazyTxnView(ddlChange.getTxnId(),txnSupplier,exceptionFactory);
            // A full Restore operation overwrites SPLICE_TXN, so the transaction used by the restore
            // may not be found.  Avoid the assertion to avoid crashing.
            // An example call stack can be found in https://splicemachine.atlassian.net/browse/DB-10025
            if (ddlChange.getDdlChangeType() != ENTER_RESTORE_MODE) {
                assert txn.allowsWrites() : "DDLChange " + ddlChange + " does not have a writable transaction";
            }
            DDLFilter ddlFilter = txController.newDDLFilter(txn);
            if (ddlFilter.compareTo(ddlDemarcationPoint.get()) > 0) {
                ddlDemarcationPoint.set(ddlFilter);
            }
        } catch (IOException e) {
            LOG.error("Couldn't create ddlFilter", e);
        }

    }

    private int killTimeouts(Set<DDLWatcher.DDLListener> listeners) throws IOException {
        /*
         * Kill transactions which have been timed out.
         */
        int numKilled = 0;
        Iterator<String> timeoutsIter = changeTimeouts.iterator();
        while(timeoutsIter.hasNext()){
            String changeId = timeoutsIter.next();
            DDLChange ddlChange = watchChecker.getChange(changeId);
            if (ddlChange != null && isTimeout(ddlChange)) {
                SpliceLogUtils.info(LOG, "DDLChange %s timed out.", ddlChange);
                watchChecker.killDDLTransaction(changeId);
                /*
                 * DB-3816. We need to notify our listeners that we failed
                 * an operation, to avoid leaking resources due to never telling
                 * them to stop dealing with a specified change
                 */
                for(DDLWatcher.DDLListener listener:listeners){
                    listener.changeFailed(changeId);
                }
                numKilled++;
                timeoutsIter.remove();
            }
        }
        return numKilled;
    }

    /**
     * We consider a DDL change to be timed out of the DDL txn was rolled back
     * @param ddlChange
     * @return
     * @throws IOException
     */
    protected boolean isTimeout(DDLChange ddlChange) throws IOException{
        SIDriver driver = SIDriver.driver();
        // Cannot resolve txn in restore mode
        if (driver.lifecycleManager().isRestoreMode())
            return false;
        long txnId = ddlChange.getTxnId();
        TxnStore txnStore = driver.getTxnStore();
        TxnView txn = txnStore.getTransaction(txnId);
        return (txn == null || txn.getEffectiveState() == Txn.State.ROLLEDBACK);
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


}
