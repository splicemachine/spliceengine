package com.splicemachine.derby.ddl;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.ddl.DDLMessage.*;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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

        Set<DDLChange> newChanges=new HashSet<>();
        boolean currentWasEmpty=currentDDLChanges.isEmpty();

        clearFinishedChanges(ongoingDDLChangeIds,callbacks);

        for(String changeId : ongoingDDLChangeIds){
            if(!seenDDLChanges.contains(changeId)){
                DDLChange change=watchChecker.getChange(changeId);
                LOG.debug("New change with id "+changeId+":"+change);
                newChanges.add(change);
                seenDDLChanges.add(changeId);
                /*
                if(change.isPreCommit()){
                    processPreCommitChange(change,callbacks);
                } else if(change.isPostCommit()){
                    for(DDLWatcher.DDLListener listener:callbacks){
                        listener.startChange(change);
                    }
                } else{
                    currentDDLChanges.put(changeId,change);
                    changeTimeouts.put(changeId,clock.currentTimeMillis());
                    for(DDLWatcher.DDLListener listener : callbacks){
                        listener.startChange(change);
                    }
                }
                */
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

        tentativeDDLS.put(ddlChange.getChangeId(),ddlChange);
        for(DDLWatcher.DDLListener listener:ddlListeners){
            listener.startChange(ddlChange);
        }
    }

    private void clearFinishedChanges(List<String> children,Collection<DDLWatcher.DDLListener> ddlListeners){
        /*
         * Remove DDL changes which are known to be finished.
         *
         * This is to avoid processing a DDL change twice.
         */
        for(Iterator<String> iterator=seenDDLChanges.iterator();iterator.hasNext();){
            String entry=iterator.next();
            if(!children.contains(entry)){
                LOG.debug("Removing change with id "+entry);
                changeTimeouts.remove(entry);
                currentDDLChanges.remove(entry);
                currChangeCount.decrementAndGet();
                tentativeDDLS.remove(entry);
                iterator.remove();
                // notify access manager
                for(DDLWatcher.DDLListener listener : ddlListeners){
                    listener.changeSuccessful(entry);
                }
            }
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

}
