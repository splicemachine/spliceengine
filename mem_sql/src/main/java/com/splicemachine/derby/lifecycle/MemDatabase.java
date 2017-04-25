/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.lifecycle;

import java.io.IOException;
import java.net.ServerSocket;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.ConfigurationBuilder;
import com.splicemachine.access.configuration.ConfigurationDefault;
import com.splicemachine.access.configuration.ConfigurationSource;
import com.splicemachine.access.configuration.HConfigurationDefaultsList;
import com.splicemachine.access.util.ReflectingConfigurationSource;
import com.splicemachine.concurrent.ConcurrentTicker;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.db.drda.NetworkServerControl;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.si.MemSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.MPartitionFactory;
import com.splicemachine.storage.MTxnPartitionFactory;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MemDatabase{

    private static DatabaseLifecycleManager init(int jdbcPort) throws Exception{
        //load SI
        MPipelinePartitionFactory tableFactory=new MPipelinePartitionFactory(new MTxnPartitionFactory(new MPartitionFactory()));
        MemSIEnvironment env=new MemSIEnvironment(tableFactory,new ConcurrentTicker(0L));
        MemSIEnvironment.INSTANCE = env;
        SConfiguration config = new ConfigurationBuilder().build(new HConfigurationDefaultsList().addConfig(new MemDatabaseTestConfig(jdbcPort)),
                                                                 new ReflectingConfigurationSource());

        SIDriver.loadDriver(env);
        final SIDriver driver = env.getSIDriver();
        //start the database sequence
        EngineLifecycleService els = new EngineLifecycleService(new DistributedDerbyStartup(){
            @Override
            public void distributedStart() throws IOException{
                PartitionFactory tableFactory=driver.getTableFactory();
                final PartitionAdmin admin=tableFactory.getAdmin();
                admin.newPartition().withName("SPLICE_RESTORE").create(); //create REstore
                admin.newPartition().withName("SPLICE_TXN").create();
                admin.newPartition().withName("TENTATIVE_DDL").create();
                admin.newPartition().withName("SPLICE_CONGLOMERATE").create();
                admin.newPartition().withName("SPLICE_SEQUENCES").create();
            }

            @Override
            public void markBootFinished() throws IOException{

            }

            @Override
            public boolean connectAsFirstTime(){
                return true;
            }
        },config);
        DatabaseLifecycleManager manager=DatabaseLifecycleManager.manager();
        manager.registerEngineService(els);
        manager.registerNetworkService(new NetworkLifecycleService(config));
        manager.start();
        return manager;
    }

    public static void main(String...args)throws Exception {
        init(NetworkServerControl.DEFAULT_PORTNUMBER);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
            @Override
            public void run(){
               DatabaseLifecycleManager.manager().shutdown();
            }
        }));

        while(true){
            Thread.sleep(10000);
        }
    }

    private static volatile boolean serverIsRunning = false;
    private static volatile Throwable partialShutdownException = null;

    public static int start() {
        return start(0);
    }

    public static int start(int jdbcPort) {
        if (serverIsRunning) {
            throw new IllegalStateException("server has already been started");
        } else if (partialShutdownException != null) {
            throw new IllegalStateException("server has not been shut down completely.", partialShutdownException);
        }

        try {
            if (jdbcPort <= 0) { // find a free port to listen on
                try (ServerSocket socket = new ServerSocket(0)) {
                    socket.setReuseAddress(true);
                    jdbcPort = socket.getLocalPort();
                }
            }
            DatabaseLifecycleManager dlm = init(jdbcPort);
            while (dlm.getState() != DatabaseLifecycleManager.State.RUNNING) {
                try { Thread.sleep(100); } catch (InterruptedException e) { ; }
            }
            serverIsRunning = true;
        } catch (Exception e) {
            partialShutdownException = e;
            throw new RuntimeException(e);
        }
        return jdbcPort;
    }

    public static void stop() {
        if (serverIsRunning) {
            try {
                serverIsRunning = false;
                DatabaseLifecycleManager dlm = DatabaseLifecycleManager.manager();
                dlm.shutdown();
                while (dlm.getState() != DatabaseLifecycleManager.State.SHUTDOWN) {
                    try { Thread.sleep(100); } catch (InterruptedException e) { ; }
                }
            } catch (Throwable t) {
                partialShutdownException = t;
                throw new IllegalStateException("server shutdown was interrupted with exception", t);
            }
        } else {
            throw new IllegalStateException("attempt to shutdown server when it has not yet been started.");
        }
    }

    //==============================================================================================================
    // private helper classes
    //==============================================================================================================
    private static class MemDatabaseTestConfig implements ConfigurationDefault {
        private final int jdbcPort;

        public MemDatabaseTestConfig(int jdbcPort) {
            this.jdbcPort = jdbcPort;
        }

        @Override
        public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
            // Overwritten for test
            builder.regionMaxFileSize = Long.MAX_VALUE;
            builder.ipcThreads = 100;
            builder.partitionserverPort = 16020;
            builder.storageFactoryHome = System.getProperty("user.dir");
            builder.networkBindPort = jdbcPort;
        }
    }
}
