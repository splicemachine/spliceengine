package com.splicemachine.server;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.client.SpliceClient;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.hbase.HBasePipelineEnvironment;
import com.splicemachine.derby.lifecycle.ManagerLoader;
import com.splicemachine.derby.lifecycle.MonitoredLifecycleService;
import com.splicemachine.derby.lifecycle.NetworkLifecycleService;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.PipelineEnvironmentLoadService;
import com.splicemachine.lifecycle.RegionServerLifecycle;
import com.splicemachine.pipeline.PipelineEnvironment;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Created by jleach on 3/20/18.
 */
public class SpliceMachineServer {
    private static final Logger LOG = Logger.getLogger(SpliceMachineServer.class);
    public static volatile String regionServerZNode;
    public static volatile String rsZnode;
    public static volatile boolean isDerbyJVM = false;

    private DatabaseLifecycleManager lifecycleManager;


    public static void main(String...args) throws Exception{
        isDerbyJVM = true;
        //ensure that the SI environment is booted properly
        HBaseSIEnvironment env=HBaseSIEnvironment.loadEnvironment(new SystemClock(), ZkUtils.getRecoverableZooKeeper());
        SIDriver driver = env.getSIDriver();

        //make sure the configuration is correct
        SConfiguration config=driver.getConfiguration();
        DatabaseLifecycleManager manager=DatabaseLifecycleManager.manager();
        HBaseRegionLoads.INSTANCE.startWatching();
        //register the engine boot service
        try{
            ManagerLoader.load().getEncryptionManager();
            HBaseConnectionFactory connFactory = HBaseConnectionFactory.getInstance(driver.getConfiguration());
            RegionServerLifecycle distributedStartupSequence=new RegionServerLifecycle(driver.getClock(),connFactory);
            manager.registerEngineService(new MonitoredLifecycleService(distributedStartupSequence,config,false));

            //register the pipeline driver environment load service
            manager.registerGeneralService(new PipelineEnvironmentLoadService() {
                @Override
                protected PipelineEnvironment loadPipelineEnvironment(ContextFactoryDriver cfDriver) throws IOException {
                    return HBasePipelineEnvironment.loadEnvironment(new SystemClock(),cfDriver);
                }
            });

            //register the network boot service
            manager.registerNetworkService(new NetworkLifecycleService(config));

            manager.start();
        }catch(Exception e1){
            LOG.error("Unexpected exception registering boot service", e1);
            throw new DoNotRetryIOException(e1);
        }

    }

}
