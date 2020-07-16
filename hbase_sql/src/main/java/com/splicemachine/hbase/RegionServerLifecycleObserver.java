/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.hbase;


import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.client.SpliceClient;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.hbase.HBasePipelineEnvironment;
import com.splicemachine.derby.lifecycle.ManagerLoader;
import com.splicemachine.derby.lifecycle.MonitoredLifecycleService;
import com.splicemachine.derby.lifecycle.NetworkLifecycleService;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.PipelineEnvironmentLoadService;
import com.splicemachine.lifecycle.RegionServerLifecycle;
import com.splicemachine.pipeline.PipelineEnvironment;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.si.data.hbase.ZkUpgrade;
import com.splicemachine.si.data.hbase.coprocessor.CoprocessorUtils;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This class implements both CoprocessorService and RegionServerObserver.  One instance will be created for each
 * region and one instance for the RegionServerObserver interface.  We should probably consider splitting this into
 * two classes.
 */
public class RegionServerLifecycleObserver implements RegionServerCoprocessor, RegionServerObserver, Coprocessor {
    private static final Logger LOG = Logger.getLogger(RegionServerLifecycleObserver.class);
    public static volatile String regionServerZNode;
    public static volatile String rsZnode;

    public static volatile boolean isHbaseJVM = false;

    private DatabaseLifecycleManager lifecycleManager;
    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     */
    @SuppressFBWarnings(value="ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification="intentional")
    @Override
    public void start(CoprocessorEnvironment e) throws IOException{
        try {
            isHbaseJVM= true;
            lifecycleManager = startEngine(e);
            SpliceClient.isRegionServer = true;
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException{
        try {
            SpliceLogUtils.warn(LOG, "Stopping RegionServerLifecycleObserver");
            lifecycleManager.shutdown();
            HBaseRegionLoads.INSTANCE.stopWatching();
            TransactionsWatcher.INSTANCE.stopWatching();
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }
    /* ****************************************************************************************************************/
    /*private helper methods*/
    @SuppressFBWarnings(value="RV_RETURN_VALUE_IGNORED_BAD_PRACTICE", justification="intentional")
    private DatabaseLifecycleManager startEngine(CoprocessorEnvironment e) throws IOException{
        RegionServerServices regionServerServices =(RegionServerServices)((RegionServerCoprocessorEnvironment) e).getOnlineRegions();

        rsZnode = regionServerServices.getZooKeeper().getZNodePaths().rsZNode;
        regionServerZNode = regionServerServices.getServerName().getServerName();

        //ensure that the SI environment is booted properly
        HBaseSIEnvironment env=HBaseSIEnvironment.loadEnvironment(new SystemClock(),ZkUtils.getRecoverableZooKeeper());
        SIDriver driver = env.getSIDriver();

        //make sure the configuration is correct
        SConfiguration config=driver.getConfiguration();

        DatabaseLifecycleManager manager=DatabaseLifecycleManager.manager();
        HBaseRegionLoads.INSTANCE.startWatching();
        TransactionsWatcher.INSTANCE.startWatching();
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

            env.txnStore().setOldTransactions(ZkUpgrade.getOldTransactions(config));

            //register the network boot service
            manager.registerNetworkService(new NetworkLifecycleService(config));

            String replicationPath = ReplicationUtils.getReplicationPath();

            byte[] status = ZkUtils.getRecoverableZooKeeper().exists(replicationPath, false) != null
                    ? ZkUtils.getData(replicationPath) : null;
            manager.start(status);

            return manager;
        }catch(Exception e1){
            LOG.error("Unexpected exception registering boot service", e1);
            throw new DoNotRetryIOException(e1);
        }
    }
}
