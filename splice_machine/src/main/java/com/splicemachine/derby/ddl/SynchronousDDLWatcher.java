package com.splicemachine.derby.ddl;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.si.api.filter.TransactionReadController;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Scott Fines
 *         Date: 1/15/16
 */
public class SynchronousDDLWatcher implements DDLWatcher,CommunicationListener{
    private static final Logger LOG = Logger.getLogger(AsynchronousDDLWatcher.class);


    private final Set<DDLListener> ddlListeners =new CopyOnWriteArraySet<>();
    private final DDLWatchChecker checker;
    private final DDLWatchRefresher refresher;

    public SynchronousDDLWatcher(TransactionReadController txnController,
                                  Clock clock,
                                  SConfiguration config,
                                  DDLWatchChecker ddlWatchChecker){
        long maxDdlWait = config.getLong(DDLConfiguration.MAX_DDL_WAIT)<<1;
        this.checker = ddlWatchChecker;
        this.refresher = new DDLWatchRefresher(checker,txnController,clock,maxDdlWait);
    }

    @Override
    public void start() throws StandardException{

        try{
            if(!checker.initialize(this))
                return;

            refresher.refreshDDL(ddlListeners);
        }catch(IOException e){
            LOG.error("Unable to initialize DDL refresher and checker");
        }
    }

    @Override
    public Collection<DDLMessage.DDLChange> getTentativeDDLs(){
        return refresher.tentativeDDLChanges();
    }

    @Override
    public void registerDDLListener(DDLListener listener){
        ddlListeners.add(listener);
    }

    @Override
    public void unregisterDDLListener(DDLListener listener){
        ddlListeners.remove(listener);
    }

    @Override
    public boolean canUseCache(TransactionManager xact_mgr){
        return refresher.canUseCache(xact_mgr);
    }

    @Override
    public boolean canUseSPSCache(TransactionManager txnMgr){
        return refresher.canUseSPSCache(txnMgr);
    }

    @Override
    public void onCommunicationEvent(String node){
        try{
            refresher.refreshDDL(ddlListeners);
        }catch(IOException | StandardException e){
            LOG.error("Unable to refresh DDL",e);
        }
    }
}
