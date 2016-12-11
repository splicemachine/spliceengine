/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.hbase;


import java.io.IOException;

import com.splicemachine.derby.lifecycle.ManagerLoader;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.lifecycle.MonitoredLifecycleService;
import com.splicemachine.derby.lifecycle.NetworkLifecycleService;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.RegionServerLifecycle;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.log4j.Logger;

/**
 * This class implements both CoprocessorService and RegionServerObserver.  One instance will be created for each
 * region and one instance for the RegionServerObserver interface.  We should probably consider splitting this into
 * two classes.
 */
public class RegionServerLifecycleObserver extends BaseRegionServerObserver{
    private static final Logger LOG = Logger.getLogger(RegionServerLifecycleObserver.class);
    public static volatile String regionServerZNode;
    public static volatile String rsZnode;

    public static volatile boolean isHbaseJVM = false;

    private DatabaseLifecycleManager lifecycleManager;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     */
    @Override
    public void start(CoprocessorEnvironment e) throws IOException{
        isHbaseJVM= true;
        lifecycleManager = startEngine(e);
    }

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env) throws IOException{
        lifecycleManager.shutdown();
        HBaseRegionLoads.INSTANCE.stopWatching();
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private DatabaseLifecycleManager startEngine(CoprocessorEnvironment e) throws IOException{
        RegionServerServices regionServerServices = ((RegionServerCoprocessorEnvironment) e).getRegionServerServices();

        rsZnode = regionServerServices.getZooKeeper().rsZNode;
        regionServerZNode = regionServerServices.getServerName().getServerName();

        //ensure that the SI environment is booted properly
        HBaseSIEnvironment env=HBaseSIEnvironment.loadEnvironment(new SystemClock(),ZkUtils.getRecoverableZooKeeper());
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
            manager.registerEngineService(new MonitoredLifecycleService(distributedStartupSequence,config));

            //register the network boot service
            manager.registerNetworkService(new NetworkLifecycleService(config));
            manager.start();
            return manager;
        }catch(Exception e1){
            LOG.error("Unexpected exception registering boot service", e1);
            throw new DoNotRetryIOException(e1);
        }
    }
}