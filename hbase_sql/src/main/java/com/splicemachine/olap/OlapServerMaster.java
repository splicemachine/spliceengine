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
 *
 */

package com.splicemachine.olap;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.access.util.NetworkUtils;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.tools.version.ManifestReader;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.log4j.Logger;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Created by dgomezferro on 29/08/2017.
 */
public class OlapServerMaster implements LeaderSelectorListener {
    private static final Logger LOG = Logger.getLogger(OlapServerMaster.class);
    private final AtomicBoolean end = new AtomicBoolean(false);
    private final int port;
    private final String queueName;
    private final String appId;
    private RecoverableZooKeeper rzk;
    private String queueZkPath;
    private String appZkPath;
    private Configuration conf;
    private CountDownLatch finished = new CountDownLatch(1);
    private ScheduledExecutorService ses = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("AppWatcher").build());

    UserGroupInformation ugi;
    UserGroupInformation yarnUgi;
    private AMRMClientAsync<AMRMClient.ContainerRequest> rmClient;
    private Mode mode;

    public OlapServerMaster(int port, String queueName, Mode mode, String appId) {
        this.port = port;
        this.queueName = queueName;
        this.mode = mode;
        this.appId = appId;
    }

    @Override
    @SuppressFBWarnings(value="DM_EXIT", justification = "Forcing process exit")
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        LOG.info("Taken leadership, starting OlapServer-"+queueName);

        String principal = System.getProperty("splice.spark.yarn.principal");
        String keytab = System.getProperty("splice.spark.yarn.keytab");

        if (principal != null && keytab != null) {
            LOG.info("Running kerberized");
            runKerberized(conf);
        } else {
            LOG.info("Running non kerberized");
            runNonKerberized(conf);
        }

        String root = HConfiguration.getConfiguration().getSpliceRootPath();
        String queueRoot = root + HBaseConfiguration.OLAP_SERVER_PATH + HBaseConfiguration.OLAP_SERVER_QUEUE_PATH;
        String appRoot = root + HBaseConfiguration.OLAP_SERVER_PATH + HBaseConfiguration.OLAP_SERVER_KEEP_ALIVE_PATH;
        zkSafeCreate(queueRoot);
        zkSafeCreate(appRoot);
        queueZkPath = queueRoot + "/" + queueName;
        appZkPath = appRoot + "/" + appId;

        UserGroupInformation.setLoginUser(ugi);
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            try {
                submitSparkApplication(conf);
            } catch (Exception e) {
                LOG.error("Unexpected exception when submitting Spark application with authentication", e);

                reportDiagnostics(e.getMessage());

                if (mode == Mode.YARN) {
                    rmClient.unregisterApplicationMaster(
                            FinalApplicationStatus.FAILED, "", "");
                    rmClient.stop();
                }

                throw e;
            }
            return null;
        });

        if (mode == Mode.YARN) {
            rmClient.unregisterApplicationMaster(
                    FinalApplicationStatus.SUCCEEDED, "", "");
            rmClient.stop();
        }

        finished.countDown();

        System.exit(0);
    }

    private void zkSafeCreate(String path) throws KeeperException, InterruptedException {
        ZkUtils.safeCreate(path, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void startAppWatcher() {
        long timeout = HConfiguration.getConfiguration().getOlapServerKeepAliveTimeout();
        ses.scheduleWithFixedDelay(new AppWatcher(timeout), 1, 1, TimeUnit.MINUTES);
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        LOG.trace("State changed: " + connectionState);
    }

    public enum Mode {
        YARN,
        KUBERNETES
    }

    public static void main(String[] args) throws Exception {
        try {
            final int port = Integer.parseInt(args[0]);
            final String roleName = args[1];
            final Mode mode = Mode.valueOf(args[2].toUpperCase());
            final String appId = args.length > 2 ? args[3] : null;
            new OlapServerMaster(port, roleName, mode, appId).run();
        } catch (Throwable t) {
            LOG.error("Failed due to unexpected exception, exiting forcefully", t);
        } finally {
            // Some issue prevented us from exiting normally
            System.exit(-1);
        }
    }

    private void reportDiagnostics(String diagnostics) {
        try {
            RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
            String root = HConfiguration.getConfiguration().getSpliceRootPath();

            String diagnosticsRoot = root + HBaseConfiguration.OLAP_SERVER_PATH + HBaseConfiguration.OLAP_SERVER_DIAGNOSTICS_PATH;
            zkSafeCreate(diagnosticsRoot);
            String diagnosticsPath = diagnosticsRoot + "/spark-" + queueName;

            if (rzk.exists(diagnosticsPath, false) != null) {
                rzk.setData(diagnosticsPath, com.splicemachine.primitives.Bytes.toBytes(diagnostics), -1);
            } else {
                rzk.create(diagnosticsPath, com.splicemachine.primitives.Bytes.toBytes(diagnostics), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            LOG.error("Exception while trying to report diagnostics", e);
            // ignore this exception during error reporting
        }
    }

    private void run() throws Exception {
        // Initialize clients to ResourceManager and NodeManagers
        conf = HConfiguration.unwrapDelegate();

        leaderElection();
        finished.await();
    }

    private void runKerberized(Configuration conf) throws Exception {
        UserGroupInformation.isSecurityEnabled();

        String principal = System.getProperty("splice.spark.yarn.principal");
        String keytab = System.getProperty("splice.spark.yarn.keytab");

        UserGroupInformation original = UserGroupInformation.getCurrentUser();
        try {
            LOG.info("Login with principal (" + principal +") and keytab (" + keytab +")");
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        } catch (IOException e) {
            LOG.error("Error while authenticating user " + principal + " with keytab " + keytab, e);
            throw new RuntimeException(e);
        }

        yarnUgi = UserGroupInformation.getCurrentUser();
        SparkHadoopUtil.get().transferCredentials(original, yarnUgi);

        rmClient = yarnUgi.doAs(
                new PrivilegedExceptionAction<AMRMClientAsync<AMRMClient.ContainerRequest>>() {
                    @Override
                    public AMRMClientAsync<AMRMClient.ContainerRequest> run() throws Exception {
                        return initClient(conf);
                    }
                });

        LOG.info("Registered with Resource Manager");

        try {
            LOG.info("Login with principal (" + principal +") and keytab (" + keytab +")");
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
        } catch (IOException e) {
            LOG.error("Error while authenticating user " + principal + " with keytab " + keytab, e);
            throw new RuntimeException(e);
        }
    }

    private void runNonKerberized(Configuration conf) throws Exception {
        // Original user has the YARN tokens
        UserGroupInformation original = UserGroupInformation.getCurrentUser();

        String user = System.getProperty("splice.spark.yarn.user", "hbase");
        LOG.info("Login with user");
        ugi = UserGroupInformation.createRemoteUser(user);
        Collection<Token<? extends TokenIdentifier>> tokens = UserGroupInformation.getCurrentUser().getCredentials().getAllTokens();
        for (Token<? extends TokenIdentifier> token : tokens) {
            LOG.debug("Token kind is " + token.getKind().toString()
                    + " and the token's service name is " + token.getService());
            if (AMRMTokenIdentifier.KIND_NAME.equals(token.getKind())) {
                ugi.addToken(token);
            }
        }

        // Transfer tokens from original user to the one we'll use from now on
        SparkHadoopUtil.get().transferCredentials(original, ugi);

        UserGroupInformation.isSecurityEnabled();
        if (mode == Mode.YARN) {
            rmClient = ugi.doAs(new PrivilegedExceptionAction<AMRMClientAsync<AMRMClient.ContainerRequest>>() {
                @Override
                public AMRMClientAsync<AMRMClient.ContainerRequest> run() throws Exception {
                    return initClient(conf);
                }
            });
            LOG.info("Registered with Resource Manager");
        }
    }

    private void leaderElection() {
        String ensemble = ZKConfig.getZKQuorumServersString(conf);
        CuratorFramework client = CuratorFrameworkFactory.newClient(ensemble, new ExponentialBackoffRetry(1000, 3));

        client.start();

        String leaderElectionPath = HConfiguration.getConfiguration().getSpliceRootPath()
                + HBaseConfiguration.OLAP_SERVER_PATH + HBaseConfiguration.OLAP_SERVER_LEADER_ELECTION_PATH
                + "/" + queueName;

        LeaderSelector leaderSelector = new LeaderSelector(client, leaderElectionPath, this);
        LOG.info("Starting leader election for OlapServer-"+queueName);
        leaderSelector.start();
    }


    private void submitSparkApplication(Configuration conf) throws IOException, InterruptedException, KeeperException {
        rzk = ZKUtil.connect(conf, null);

        if (mode == Mode.YARN) {
            startAppWatcher();
        }

        HBaseSIEnvironment env=HBaseSIEnvironment.loadEnvironment(new SystemClock(),rzk);

        SpliceSpark.setupSpliceStaticComponents();

        LOG.info("Spark static components loaded");

        OlapServer server = new OlapServer(port, env.systemClock());
        server.startServer(env.configuration());
        LOG.info("OlapServer started");

        int port = server.getBoundPort();
        String hostname = NetworkUtils.getHostname(HConfiguration.getConfiguration());

        publishServer(rzk, hostname, port);

        SpliceSpark.getContextUnsafe(); // kickstart Spark

        while(!end.get()) {
            Thread.sleep(10000);
            ugi.checkTGTAndReloginFromKeytab();
            if (yarnUgi != null)
                yarnUgi.checkTGTAndReloginFromKeytab();
        }

        LOG.info("OlapServerMaster shutting down");
    }

    private void publishServer(RecoverableZooKeeper rzk, String hostname, int port) throws InterruptedException, KeeperException {
        try {
            HostAndPort hostAndPort = HostAndPort.fromParts(hostname, port);
            queueZkPath = rzk.getZooKeeper().create(queueZkPath, Bytes.toBytes(hostAndPort.toString()),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            rzk.getData(queueZkPath, new QueueWatcher(), null);
        } catch (Exception e) {
            LOG.error("Couldn't register OlapServer due to unexpected exception", e);
            throw e;
        }
    }


    private AMRMClientAsync<AMRMClient.ContainerRequest> initClient(Configuration conf) throws YarnException, IOException {
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

        return rmClient;
    }

    class QueueWatcher implements Watcher {
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
                        rzk.getData(queueZkPath, this, null);
                        return;
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

    class AppWatcher implements Runnable {
        private final DatabaseVersion version;
        private long latestTimestamp;
        private long timeout;

        public AppWatcher(long timeoutInSeconds) {
            this.timeout = timeoutInSeconds * 1000;
            latestTimestamp = System.currentTimeMillis();
            ManifestReader reader = new ManifestReader();
            this.version = reader.createVersion();
        }

        @Override
        public void run() {
            try {
                byte[] payload = rzk.getData(appZkPath, false, null);
                OlapMessage.KeepAlive msg = OlapMessage.KeepAlive.parseFrom(payload);
                latestTimestamp = msg.getTime();
                if (msg.getMajor() != version.getMajorVersionNumber()
                        || msg.getMinor() != version.getMinorVersionNumber()
                        || msg.getPatch() != version.getPatchVersionNumber()
                        || msg.getSprint() != version.getSprintVersionNumber()
                        || !msg.getImplementation().equals(version.getImplementationVersion())) {
                    LOG.info("New version detected, restarting OlapServer-" + queueName);
                    end.set(true);
                    return;
                }
            } catch (Exception e) {
                LOG.warn("KeepAlive failed, retrying later", e);
            }
            long diff = System.currentTimeMillis() - latestTimestamp;
            if (diff > timeout) {
                LOG.error("Time out reached after " + diff + " milliseconds, stopping OlapServer-" + queueName);
                end.set(true);
            }
            if (diff > TimeUnit.MINUTES.toMillis(2)) {
                LOG.warn("Keep alive is delayed for " + diff + " milliseconds, stopping OlapServer-" + queueName + " when it reaches " + timeout);
            }
        }
    }
}

