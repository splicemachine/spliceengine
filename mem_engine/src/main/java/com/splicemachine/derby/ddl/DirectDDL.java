package com.splicemachine.derby.ddl;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.ddl.DDLMessage;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public class DirectDDL implements DDLController,DDLWatcher{
    private List<DDLListener> listeners = new CopyOnWriteArrayList<>();

    private Map<String,DDLMessage.DDLChange> changesInFlight = new ConcurrentHashMap<>();
    private Map<String,DDLMessage.DDLChange> tentativeDDLS = new ConcurrentHashMap<>();

    @Override
    public String notifyMetadataChange(DDLMessage.DDLChange change) throws StandardException{
        String changeId = change.hasChangeId()? change.getChangeId(): "change-"+change.getDdlChangeType()+"-"+change.getTxnId();
        boolean hadPrevious = !changesInFlight.isEmpty();
        changesInFlight.put(changeId,change);
        tentativeDDLS.put(changeId,change);
        for(DDLListener listener:listeners){
            listener.startChange(change);
        }
        if(!hadPrevious){
            for(DDLListener listener:listeners){
                listener.startGlobalChange();
            }
        }

        return changeId;
    }

    @Override
    public void finishMetadataChange(String changeId) throws StandardException{
        DDLMessage.DDLChange change = changesInFlight.get(changeId);
        for(DDLListener listener:listeners){
            listener.changeSuccessful(changeId,change);
        }
        changesInFlight.remove(changeId);
        if(changesInFlight.isEmpty()){
            for(DDLListener listener:listeners){
                listener.finishGlobalChange();
            }

        }
    }

    @Override
    public void start() throws StandardException{

    }

    @Override
    public Collection<DDLMessage.DDLChange> getTentativeDDLs(){
        //TODO -sf- this probably isn't exactly right either
        return Collections.emptyList();
    }

    @Override
    public void registerDDLListener(DDLListener listener){
        this.listeners.add(listener);
    }

    @Override
    public void unregisterDDLListener(DDLListener listener){
        this.listeners.remove(listener);
    }

    @Override
    public boolean canUseCache(TransactionManager xact_mgr){
        return changesInFlight.size()<=0;
    }
}
