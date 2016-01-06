package com.splicemachine.hbase;

import com.splicemachine.SQLConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.lifecycle.EngineLifecycleService;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.MasterLifecycle;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Responsible for actions (create system tables, restore tables) that should only happen on one node.
 */
public class SpliceMasterObserver extends BaseMasterObserver {

    private static final Logger LOG = Logger.getLogger(SpliceMasterObserver.class);

    public static final byte[] INIT_TABLE = Bytes.toBytes("SPLICE_INIT");


    @Override
    public void start(CoprocessorEnvironment ctx) throws IOException {
        LOG.info("Starting SpliceMasterObserver");

    }

    @Override
    public void stop(CoprocessorEnvironment ctx) throws IOException {
        LOG.warn("Stopping SpliceMasterObserver");
        DatabaseLifecycleManager.manager().shutdown();
    }

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
        SpliceLogUtils.info(LOG, "preCreateTable %s", Bytes.toString(desc.getTableName().getName()));
        if (Bytes.equals(desc.getTableName().getName(), INIT_TABLE)) {
            DatabaseLifecycleManager manager=DatabaseLifecycleManager.manager();
            switch(manager.getState()){
                case NOT_STARTED:
                    boot();
                case BOOTING_ENGINE:
                case BOOTING_GENERAL_SERVICES:
                case BOOTING_SERVER:
                    throw new PleaseHoldException("Please Hold - Starting");
                case RUNNING:
                    throw new DoNotRetryIOException("Success");
                case STARTUP_FAILED:
                case SHUTTING_DOWN:
                case SHUTDOWN:
                    throw new IllegalStateException("Startup failed");
            }
        }
    }

    private void boot() throws IOException{
        //make sure the SIDriver is booted

        //ensure that the SI environment is booted properly
        HBaseSIEnvironment env=HBaseSIEnvironment.loadEnvironment(new SystemClock(),ZkUtils.getRecoverableZooKeeper());
        SIDriver driver = env.getSIDriver();

        //make sure the configuration is correct
        SConfiguration config=driver.getConfiguration();
        config.addDefaults(SQLConfiguration.defaults);

        DatabaseLifecycleManager manager=DatabaseLifecycleManager.manager();
        //register the engine boot service
        try{
            MasterLifecycle distributedStartupSequence=new MasterLifecycle();
            manager.registerEngineService(new EngineLifecycleService(distributedStartupSequence,config));
            manager.start();
        }catch(Exception e1){
            throw new DoNotRetryIOException(e1);
        }
    }

}