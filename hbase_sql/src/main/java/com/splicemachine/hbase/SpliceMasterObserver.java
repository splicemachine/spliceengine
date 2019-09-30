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

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.lifecycle.EngineLifecycleService;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.MasterLifecycle;
import com.splicemachine.olap.OlapServer;
import com.splicemachine.lifecycle.DatabaseLifecycleService;
import com.splicemachine.olap.OlapServerSubmitter;
import com.splicemachine.pipeline.InitializationCompleted;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.coprocessor.CoprocessorUtils;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.timestamp.hbase.ZkTimestampBlockManager;
import com.splicemachine.timestamp.impl.TimestampServer;
import com.splicemachine.timestamp.impl.TimestampServerHandler;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Responsible for actions (create system tables, restore tables) that should only happen on one node.
 */
public class SpliceMasterObserver extends BaseMasterObserver {

    private static final Logger LOG = Logger.getLogger(SpliceMasterObserver.class);

    public static final byte[] INIT_TABLE = Bytes.toBytes("SPLICE_INIT");

    private TimestampServer timestampServer;
    private DatabaseLifecycleManager manager;
    private OlapServer olapServer;

    @Override
    public void start(CoprocessorEnvironment ctx) throws IOException {
        try {
            LOG.info("Starting SpliceMasterObserver");

            LOG.info("Starting Timestamp Master Observer");

            ZooKeeperWatcher zkw = ((MasterCoprocessorEnvironment)ctx).getMasterServices().getZooKeeper();
            RecoverableZooKeeper rzk = zkw.getRecoverableZooKeeper();

            HBaseSIEnvironment env=HBaseSIEnvironment.loadEnvironment(new SystemClock(),rzk);
            SConfiguration configuration=env.configuration();

            String timestampReservedPath=configuration.getSpliceRootPath()+HConfiguration.MAX_RESERVED_TIMESTAMP_PATH;
            int timestampPort=configuration.getTimestampServerBindPort();
            int timestampBlockSize = configuration.getTimestampBlockSize();

            TimestampBlockManager tbm= new ZkTimestampBlockManager(rzk,timestampReservedPath);
            this.timestampServer =new TimestampServer(timestampPort,new TimestampServerHandler(tbm, timestampBlockSize));

            this.timestampServer.startServer();

            if (!configuration.getOlapServerExternal()) {
                int olapPort = configuration.getOlapServerBindPort();
                this.olapServer = new OlapServer(olapPort, env.systemClock());
                this.olapServer.startServer(configuration);
            }

            /*
             * We create a new instance here rather than referring to the singleton because we have
             * a problem when booting the master and the region server in the same JVM; the singleton
             * then is unable to boot on the master side because the regionserver has already started it.
             *
             * Generally, this isn't a problem because the underlying singleton is constructed on demand, so we
             * will still only create a single manager per JVM in a production environment, and we avoid the deadlock
             * issue during testing
             */
            this.manager = new DatabaseLifecycleManager();
            super.start(ctx);
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment ctx) throws IOException {
        try {
            LOG.warn("Stopping SpliceMasterObserver");
            manager.shutdown();
            this.timestampServer.stopServer();
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
        try {
            SpliceLogUtils.info(LOG, "preCreateTable %s", Bytes.toString(desc.getTableName().getName()));
            if (Bytes.equals(desc.getTableName().getName(), INIT_TABLE)) {
                switch(manager.getState()){
                    case NOT_STARTED:
                        throw new PleaseHoldException("Please Hold - Master not started");
                    case BOOTING_ENGINE:
                    case BOOTING_GENERAL_SERVICES:
                    case BOOTING_SERVER:
                        throw new PleaseHoldException("Please Hold - Starting");
                    case RUNNING:
                        throw new InitializationCompleted("Success");
                    case STARTUP_FAILED:
                    case SHUTTING_DOWN:
                    case SHUTDOWN:
                        throw new IllegalStateException("Startup failed");
                }
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        try {
            boot(ctx.getEnvironment().getMasterServices().getServerName());
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    private synchronized void boot(ServerName serverName) throws IOException{
        //make sure the SIDriver is booted
        if (! manager.getState().equals(DatabaseLifecycleManager.State.NOT_STARTED))
            return; // Race Condition, only load one...

        if (HConfiguration.getConfiguration().getOlapServerExternal()) {
            try {
                manager.registerNetworkService(new DatabaseLifecycleService() {
                    List<OlapServerSubmitter> serverSubmitters;

                    @Override
                    public void start() throws Exception {
                        Collection<String> queues = HConfiguration.getConfiguration().getOlapServerYarnQueues().keySet();
                        serverSubmitters = new ArrayList<>();
                        Set<String> names = new HashSet<>(queues);
                        names.add(SIConstants.OLAP_DEFAULT_QUEUE_NAME);
                        if (HConfiguration.getConfiguration().getOlapServerIsolatedCompaction()) {
                            names.add(HConfiguration.getConfiguration().getOlapServerIsolatedCompactionQueueName());
                        }

                        for (String queue : names) {
                            OlapServerSubmitter oss = new OlapServerSubmitter(serverName, queue);
                            serverSubmitters.add(oss);
                            Thread thread = new Thread(oss, "OlapServerSubmitter-"+queue);
                            thread.setDaemon(true);
                            thread.start();
                        }
                    }

                    @Override
                    public void registerJMX(MBeanServer mbs) throws Exception {
                    }

                    @Override
                    public void shutdown() throws Exception {
                        for (OlapServerSubmitter oss : serverSubmitters) {
                            oss.stop();
                        }
                    }
                });
            } catch(Exception e){
                LOG.error("Unexpected exception registering Olap Server service", e);
                throw new DoNotRetryIOException(e);
            }
        }

        //make sure only one master boots at a time
        String lockPath = HConfiguration.getConfiguration().getSpliceRootPath()+HConfiguration.MASTER_INIT_PATH;
        SpliceMasterLock lock = new SpliceMasterLock(HConfiguration.getConfiguration().getSpliceRootPath(), lockPath, ZkUtils.getRecoverableZooKeeper());
        IOException exception = null;
        try {
            lock.acquire();

            //ensure that the SI environment is booted properly
            HBaseSIEnvironment env = HBaseSIEnvironment.loadEnvironment(new SystemClock(), ZkUtils.getRecoverableZooKeeper());
            SIDriver driver = env.getSIDriver();

            //make sure the configuration is correct
            SConfiguration config = driver.getConfiguration();

            //register the engine boot service
            try {
                MasterLifecycle distributedStartupSequence = new MasterLifecycle();
                manager.registerEngineService(new EngineLifecycleService(distributedStartupSequence, config, true));
                manager.start();
            } catch (Exception e1) {
                LOG.error("Unexpected exception registering boot service", e1);
                throw new DoNotRetryIOException(e1);
            }
        } catch (IOException e) {
            exception = e;
            throw exception;
        } catch (Exception e) {
            exception = new IOException("Error locking " + lockPath + " for master initialization", e);
            throw exception;
        } finally {
            if (lock.isAcquired()) {
                try {
                    lock.release();
                } catch (Exception e) {
                    if (exception != null)
                        throw exception;
                    else
                        throw new IOException("Error releasing " + lockPath + " after master initialization", e);
                }
            }
        }
    }

}
