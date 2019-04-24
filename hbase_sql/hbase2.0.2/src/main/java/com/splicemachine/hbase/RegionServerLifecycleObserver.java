/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HBasePlatformUtils;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This class implements both CoprocessorService and RegionServerObserver.  One instance will be created for each
 * region and one instance for the RegionServerObserver interface.  We should probably consider splitting this into
 * two classes.
 */
public class RegionServerLifecycleObserver extends BaseRegionServerLifecycleObserver implements RegionServerCoprocessor{
    private static final Logger LOG = Logger.getLogger(RegionServerLifecycleObserver.class);

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     */
    @Override
    public void start(CoprocessorEnvironment e) throws IOException{
        startAction(e);
    }

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env) throws IOException{
        preStopRegionServerAction(env);
    }
}
