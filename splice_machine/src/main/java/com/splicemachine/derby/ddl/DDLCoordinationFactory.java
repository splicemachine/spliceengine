package com.splicemachine.derby.ddl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.splicemachine.concurrent.ReentrantLockFactory;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.ddl.TentativeDDLDesc;

public class DDLCoordinationFactory {

    public static final Gson GSON = new GsonBuilder().
            registerTypeAdapter(TentativeDDLDesc.class, new InterfaceSerializer<TentativeDDLDesc>()).
            registerTypeAdapter(UUID.class, new InterfaceSerializer<UUID>()).
            create();

    private static final DDLController DDL_CONTROLLER = new AsynchronousDDLController(new ZooKeeperDDLCommunicator(),
            ReentrantLockFactory.instance(),SystemClock.INSTANCE);
    private static volatile DDLWatcher DDL_WATCHER = new ZookeeperDDLWatcher();

    public static DDLController getController() {
        return DDL_CONTROLLER;
    }

    public static DDLWatcher getWatcher() {
        DDLWatcher watcher = DDL_WATCHER;
        if(watcher==null){
            watcher = initializeWatcher();
        }
        return watcher;
    }

    private synchronized static DDLWatcher initializeWatcher() {
        if(DDL_WATCHER!=null) return DDL_WATCHER;
        if (Monitor.getMonitor() == null) {
            // can't initialize yet
            return null;
        }
        SpliceDatabase db = ((SpliceDatabase) Monitor.findService(Property.DATABASE_MODULE, SpliceConstants.SPLICE_DB));
        if (db == null) {
            // can't initialize yet
            return null;
        }

        final SpliceAccessManager accessManager;
        try {
            accessManager = (SpliceAccessManager) Monitor.findServiceModule(db, AccessFactory.MODULE);
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
        DDLWatcher watcher = new ZookeeperDDLWatcher();
        watcher.registerDDLListener(new DDLWatcher.DDLListener() {
            @Override public void startGlobalChange() {  }
            @Override public void finishGlobalChange() {  }

            @Override
            public void changeFailed(String changeId){
               accessManager.cancelDDLChange(changeId);
            }

            @Override
            public void startChange(DDLChange change) {
                accessManager.startDDLChange(change);
            }

            @Override
            public void changeSuccessful(String changeId) {
                accessManager.finishDDLChange(changeId);
            }
        });
        DDL_WATCHER = watcher;
        return watcher;
    }
}
