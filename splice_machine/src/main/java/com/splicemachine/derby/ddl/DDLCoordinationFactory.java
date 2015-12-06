package com.splicemachine.derby.ddl;

import com.splicemachine.concurrent.ReentrantLockFactory;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.database.Database;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.monitor.Monitor;

public class DDLCoordinationFactory {

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
        DDLWatcher watcher = new ZookeeperDDLWatcher();
        DDL_WATCHER = watcher;
        return watcher;
    }

    public boolean canUseCache(TransactionManager transactionManager) {
        return getWatcher().canUseCache(transactionManager);
    }

}