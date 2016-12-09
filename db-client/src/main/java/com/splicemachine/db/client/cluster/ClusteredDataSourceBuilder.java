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
 *
 */

package com.splicemachine.db.client.cluster;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Date: 9/15/16
 */
@SuppressWarnings("WeakerAccess")
public class ClusteredDataSourceBuilder{
    private String[] initialServers;
    private long discoveryWindow;
    private int numHeartbeatThreads = 1;
    private long heartbeatPeriod;
    private ConnectionSelectionStrategy css;
    private ServerPoolFactory spf;
    private ServerDiscovery serverDiscovery;
    private boolean validateConnections = false;

    ClusteredDataSourceBuilder(){}

    public ClusteredDataSourceBuilder servers(String... servers){
        this.initialServers=servers;
        return this;
    }

    public ClusteredDataSourceBuilder discoveryWindow(long discoveryWindow){
        this.discoveryWindow=discoveryWindow;
        return this;
    }

    public ClusteredDataSourceBuilder schedulerThreads(int numHeartbeatThreads){
        this.numHeartbeatThreads=numHeartbeatThreads;
        return this;
    }

    public ClusteredDataSourceBuilder heartbeatPeriod(long heartbeatPeriod){
        this.heartbeatPeriod = heartbeatPeriod;
        return this;
    }

    public ClusteredDataSourceBuilder connectionSelection(ConnectionSelectionStrategy css){
        this.css = css;
        return this;
    }

    public ClusteredDataSourceBuilder serverPoolFactory(ServerPoolFactory spf){
        this.spf=spf;
        return this;
    }

    public ClusteredDataSourceBuilder serverDiscovery(ServerDiscovery serverDiscovery){
        this.serverDiscovery=serverDiscovery;
        return this;
    }

    public ClusteredDataSourceBuilder setValidateConnections(boolean validateConnections){
        this.validateConnections=validateConnections;
        return this;
    }

    public ClusteredDataSource build(){
        ServerPool[] serverPools = new ServerPool[initialServers.length];
        for(int i=0;i<initialServers.length;i++){
            serverPools[i] =spf.newServerPool(initialServers[i]);
        }
        ServerList sl = new ServerList(css,serverPools);

        if(serverDiscovery==null){
            serverDiscovery = new ConnectionServerDiscovery(initialServers,sl,spf);
        }

        ScheduledExecutorService ses =Executors.newScheduledThreadPool(numHeartbeatThreads,new ThreadFactory(){
            private final AtomicInteger counter = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r){
                Thread t = new Thread(r);
                t.setName("cluster-heartbeat-"+counter.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });

        return new ClusteredDataSource(sl,spf,serverDiscovery,
                ses,validateConnections,discoveryWindow,heartbeatPeriod);
    }
}
