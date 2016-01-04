package com.splicemachine.ddl;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.LockFactory;
import com.splicemachine.concurrent.ReentrantLockFactory;
import com.splicemachine.derby.ddl.*;
import com.splicemachine.si.api.filter.TransactionReadController;

/**
 * @author Scott Fines
 *         Date: 1/4/16
 */
public class HBaseDDLEnvironment implements DDLEnvironment{

    private DDLController ddlController;
    private DDLWatcher watcher;
    private SConfiguration config;


    public HBaseDDLEnvironment(Clock clock,
                               TransactionReadController txnController,
                               SConfiguration configuration){
        configuration.addDefaults(DDLConfiguration.defaults); //make sure that we have the defaults in place
        ZooKeeperDDLCommunicator communicator=new ZooKeeperDDLCommunicator();
        LockFactory lf = new ReentrantLockFactory(false);
        this.ddlController = new AsynchronousDDLController(communicator,lf,clock,configuration);
        this.watcher = new ZookeeperDDLWatcher(txnController,clock,configuration);
        this.config = configuration;
    }

    @Override
    public DDLController getController(){
        return ddlController;
    }

    @Override
    public DDLWatcher getWatcher(){
        return watcher;
    }

    @Override
    public SConfiguration getConfiguration(){
        return config;
    }
}
