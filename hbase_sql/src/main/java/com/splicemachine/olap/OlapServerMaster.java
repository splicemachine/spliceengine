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
 *
 */

package com.splicemachine.olap;

import com.google.common.net.HostAndPort;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.access.util.NetworkUtils;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.log4j.Logger;
import org.apache.spark.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.zookeeper.KeeperException.Code.NODEEXISTS;
import static org.apache.zookeeper.KeeperException.Code.NONODE;


/**
 * Created by dgomezferro on 29/08/2017.
 */
public class OlapServerMaster implements Watcher {
    private static final Logger LOG = Logger.getLogger(OlapServerMaster.class);
    private final ServerName serverName;
    private final AtomicBoolean end = new AtomicBoolean(false);
    private final int port;
    private RecoverableZooKeeper rzk;
    private String masterPath;

    public OlapServerMaster(ServerName serverName, int port) {
        this.serverName = serverName;
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        try {
            final ServerName serverName = ServerName.parseServerName(args[0]);
            final int port = Integer.parseInt(args[1]);
            new OlapServerMaster(serverName, port).run();
        } finally {
            // Some issue prevented us from exiting normally
            System.exit(-1);
        }
    }

    private void run() throws Exception {

        // Initialize clients to ResourceManager and NodeManagers
        Configuration conf = HConfiguration.unwrapDelegate();

        AMRMClientAsync.CallbackHandler allocListener = new AMRMClientAsync.CallbackHandler() {
            @Override
            public void onContainersCompleted(List<ContainerStatus> statuses) {
            }

            @Override
            public void onContainersAllocated(List<Container> containers) {
            }

            @Override
            public void onShutdownRequest() {
                LOG.warn("Shutting down");
                end.set(true);
            }

            @Override
            public void onNodesUpdated(List<NodeReport> updatedNodes) {
            }

            @Override
            public float getProgress() {
                return 0;
            }

            @Override
            public void onError(Throwable e) {
                LOG.error("Unexpected error", e);
                end.set(true);
            }
        };
        AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        rmClient.init(conf);
        rmClient.start();

        // Register with ResourceManager
        rmClient.registerApplicationMaster(Utils.localHostName(), 0, "");

        LOG.info("Registered with Resource Manager");

        String principal = System.getProperty("splice.spark.yarn.principal");
        String keytab = System.getProperty("splice.spark.yarn.keytab");

        UserGroupInformation ugi;

        if (principal != null && keytab != null) {
                try {
                    LOG.info("Login with principal (" + principal +") and keytab (" + keytab +")");
                    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
                } catch (IOException e) {
                    LOG.error("Error while authenticating user " + principal + " with keytab " + keytab, e);
                    throw new RuntimeException(e);
                }
        } else {
            String user = System.getProperty("splice.spark.yarn.user", "hbase");
            LOG.info("Login with user");
            ugi = UserGroupInformation.createRemoteUser(user);
        }

        UserGroupInformation.setLoginUser(ugi);
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            try {
                submitSparkApplication(conf, ugi);
            } catch (Exception e) {
                LOG.error("Unexpected exception when submitting Spark application with authentication", e);

                rmClient.unregisterApplicationMaster(
                        FinalApplicationStatus.FAILED, "", "");
                rmClient.stop();

                throw e;
            }
            return null;
        });

        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        rmClient.stop();

        System.exit(0);
    }

    private void submitSparkApplication(Configuration conf, UserGroupInformation ugi) throws IOException, InterruptedException, KeeperException {
        rzk = ZKUtil.connect(conf, null);

        HBaseSIEnvironment env=HBaseSIEnvironment.loadEnvironment(new SystemClock(),rzk);

        HBaseConnectionFactory hbcf = HBaseConnectionFactory.getInstance(HConfiguration.getConfiguration());
        Timer timer = new Timer("HMaster-checker", true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (!serverName.equals(hbcf.getMasterServer()))
                        end.set(true);
                } catch (Throwable t) {
                    LOG.error("Got exception while checking HMaster status, aborting OlapServerMaster", t);
                    end.set(true);
                }

            }
        }, 10000, 10000);

        SpliceSpark.setupSpliceStaticComponents();

        LOG.info("Spark static components loaded");

        OlapServer server = new OlapServer(port, env.systemClock());
        server.startServer(env.configuration());
        LOG.info("OlapServer started");

        int port = server.getBoundPort();
        String hostname = NetworkUtils.getHostname(HConfiguration.getConfiguration());

        publishServer(rzk, serverName, hostname, port);

        SpliceSpark.getContextUnsafe(); // kickstart Spark
        
        while(!end.get()) {
            Thread.sleep(10000);
            ugi.checkTGTAndReloginFromKeytab();
        }

        LOG.info("OlapServerMaster shutting down");
    }

    private void publishServer(RecoverableZooKeeper rzk, ServerName serverName, String hostname, int port) throws InterruptedException, KeeperException {
        String root = HConfiguration.getConfiguration().getSpliceRootPath();

        try {
            HostAndPort hostAndPort = HostAndPort.fromParts(hostname, port);
            masterPath = root + HBaseConfiguration.OLAP_SERVER_PATH + "/" + serverName;
            rzk.create(masterPath, Bytes.toBytes(hostAndPort.toString()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            rzk.getData(masterPath, this, null);
        } catch (Exception e) {
            LOG.error("Couldn't register OlapServer due to unexpected exception", e);
            throw e;
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType().equals(Event.EventType.NodeDeleted)) {
            if (!end.get()) {
                LOG.warn("Someone deleted our published address, stopping");
                end.set(true);
            }
        } else {
            int tries = 0;
            while (true) {
                try {
                    rzk.getData(masterPath, this, null);
                } catch (Exception e) {
                    if (tries < 5) {
                        LOG.warn("Unexpected exception when setting watcher, retrying", e);
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e1) {
                            LOG.error("Interrupted, aborting", e);
                            end.set(true);
                        }
                        tries++;
                    } else {
                        LOG.error("Unexpected exception when setting watcher, aborting", e);
                        end.set(true);
                    }
                }
            }
        }
    }
}
