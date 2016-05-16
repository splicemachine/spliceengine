package com.splicemachine.ddl;

import java.io.IOException;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.LockFactory;
import com.splicemachine.concurrent.ReentrantLockFactory;
import com.splicemachine.derby.ddl.AsynchronousDDLController;
import com.splicemachine.derby.ddl.AsynchronousDDLWatcher;
import com.splicemachine.derby.ddl.DDLController;
import com.splicemachine.derby.ddl.DDLEnvironment;
import com.splicemachine.derby.ddl.DDLWatcher;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.impl.driver.SIDriver;

/**
 * @author Scott Fines
 *         Date: 1/4/16
 */
public class HBaseDDLEnvironment implements DDLEnvironment{

    private DDLController ddlController;
    private DDLWatcher watcher;
    private SConfiguration config;


    public HBaseDDLEnvironment(){ }

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

    @Override
    public void configure(SqlExceptionFactory exceptionFactory,SConfiguration config) throws IOException{
        DDLZookeeperClient zkClient = new DDLZookeeperClient(config);
        ZooKeeperDDLCommunicator communicator=new ZooKeeperDDLCommunicator(zkClient);
        LockFactory lf = new ReentrantLockFactory(false);
        Clock clock= SIDriver.driver().getClock();
        this.ddlController = new AsynchronousDDLController(communicator,lf,clock,config);
        SIDriver driver=SIDriver.driver();
        TransactionReadController txnController=driver.readController();
        this.watcher = new AsynchronousDDLWatcher(txnController,
                clock,
                config,
                new ZooKeeperDDLWatchChecker(zkClient),
                exceptionFactory,
                driver.getTxnSupplier());
        this.watcher.start();
        this.config = config;

    }
}
