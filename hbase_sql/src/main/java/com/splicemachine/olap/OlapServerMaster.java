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

import org.apache.spark.SparkConf;
import splice.com.google.common.net.HostAndPort;
import splice.com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.splicemachine.access.configuration.HBaseConfiguration.MAX_EXECUTOR_CORES;



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
    private String maxExecutorCoresZkPath;
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
        maxExecutorCoresZkPath = root + MAX_EXECUTOR_CORES;

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

        JavaSparkContext sparkContext = SpliceSpark.getContextUnsafe(); // kickstart Spark
        publishMaxExecutorCores(rzk, sparkContext.sc());

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

    private String
    getSparkProperty(String key, SparkConf sparkConf) {
        String result = null;
        try {
            result = sparkConf.get(key);
        }
        catch (Exception e) {

        }
        return result;
    }

    private String
    getYarnProperty(String key, Configuration yarnConf) {
        String result = null;
        try {
            result = yarnConf.get(key);
        }
        catch (Exception e) {

        }
        return result;
    }

    private void publishMaxExecutorCores(RecoverableZooKeeper rzk, SparkContext sparkContext) throws InterruptedException, KeeperException, IOException {


        try {
            Configuration yarnConf = HConfiguration.unwrapDelegate();
            String yarnMemory = getYarnProperty("yarn.nodemanager.resource.memory-mb", yarnConf);
            if (yarnMemory == null || "-1".equals(yarnMemory))
                yarnMemory = getYarnProperty("yarn.scheduler.maximum-allocation-mb", yarnConf);

            SparkConf sparkConf = sparkContext.conf();

            Integer numNodes = rmClient.getClusterNodeCount();
            if (numNodes < 1)
                numNodes = 1;

            int maxExecutorCores =
                calculateMaxExecutorCores(yarnMemory,
                                          getSparkProperty("spark.dynamicAllocation.enabled", sparkConf),
                                          getSparkProperty("spark.executor.instances", sparkConf),
                                          getSparkProperty("spark.executor.cores", sparkConf),
                                          getSparkProperty("spark.executor.memory", sparkConf),
                                          getSparkProperty("spark.dynamicAllocation.maxExecutors", sparkConf),
                                          getSparkProperty("spark.executor.memoryOverhead", sparkConf),
                                          getSparkProperty("spark.yarn.executor.memoryOverhead", sparkConf),
                                          numNodes);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeInt(maxExecutorCores);
            oos.writeInt(numNodes);
            oos.flush();
            byte [] serializedConfig = bos.toByteArray();

            ZkUtils.safeDelete(maxExecutorCoresZkPath, -1, rzk);
            ZkUtils.safeCreate(maxExecutorCoresZkPath, serializedConfig, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            LOG.error("Couldn't register maxExecutorCores due to unexpected exception", e);
            throw e;
        }
    }

    /**
     * Estimate the maximum number of Spark executor cores that could be running
     * simultaneously given the current YARN and splice.spark settings.
     *
     * @return The maximum number of Spark executor cores.
     * @notes See @link https://spark.apache.org/docs/latest/configuration.html
     *        for more information on spark configuration properties.
     */
    public static int
    calculateMaxExecutorCores(String memorySize,                         // yarn.nodemanager.resource.memory-mb
                              String sparkDynamicAllocationString,       // splice.spark.dynamicAllocation.enabled
                              String executorInstancesString,            // splice.spark.executor.instances
                              String executorCoresString,                // splice.spark.executor.cores
                              String sparkExecutorMemory,                // splice.spark.executor.memory
                              String sparkDynamicAllocationMaxExecutors, // splice.spark.dynamicAllocation.maxExecutors
                              String sparkExecutorMemoryOverhead,        // splice.spark.executor.memoryOverhead
                              String sparkYARNExecutorMemoryOverhead,    // splice.spark.yarn.executor.memoryOverhead
                              int numNodes)
    {
        if (numNodes < 1)
            numNodes = 1;

        int executorCores = 1;
        if (executorCoresString != null) {
            try {
                executorCores = Integer.parseInt(executorCoresString);
                if (executorCores < 1)
                    executorCores = 1;
            } catch (NumberFormatException e) {
            }
        }

        // Initialize to defaults, then check for custom settings.
        long memSize = 8192L*1024*1024, containerSize;
        if (memorySize != null) {
            try {
                memSize = Long.parseLong(memorySize) * 1024 * 1024;
            }
            catch (NumberFormatException e) {
            }
            if (memSize <= 0)
                memSize = 8192L*1024*1024;
        }

        boolean dynamicAllocation = sparkDynamicAllocationString != null &&
                                    Boolean.parseBoolean(sparkDynamicAllocationString);
        int numSparkExecutorCores = executorCores;

        int numSparkExecutors = 0;
        if (!dynamicAllocation) {
            try {
                numSparkExecutors = 1;
                numSparkExecutors = Integer.parseInt(executorInstancesString);
                if (numSparkExecutors < 1)
                    numSparkExecutors = 1;
                numSparkExecutorCores = numSparkExecutors * executorCores;

            } catch (NumberFormatException e) {
            }
        }
        else {
            numSparkExecutors = Integer.MAX_VALUE;
            if (sparkDynamicAllocationMaxExecutors != null) {
                try {
                    numSparkExecutors = Integer.parseInt(sparkDynamicAllocationMaxExecutors);
                    if (numSparkExecutors <= 0)
                        numSparkExecutors = Integer.MAX_VALUE;
                }
                catch(NumberFormatException e){
                }
            }
        }

        long executorMemory = 1024L * 1024 * 1024; // 1g
        if (sparkExecutorMemory != null)
            executorMemory =
                parseSizeString(sparkExecutorMemory, executorMemory, "b");

        long sparkMemOverhead = executorMemory / 10;
        if (sparkExecutorMemoryOverhead != null) {
            try {
                sparkMemOverhead =
                    parseSizeString(sparkExecutorMemoryOverhead, sparkMemOverhead, "m");
            } catch (NumberFormatException e) {
            }
        }
        long sparkMemOverheadLegacy = executorMemory / 10;
        if (sparkYARNExecutorMemoryOverhead != null) {
            try {
                sparkMemOverheadLegacy =
                    parseSizeString(sparkYARNExecutorMemoryOverhead, sparkMemOverheadLegacy, "m");
            } catch (NumberFormatException e) {
            }
        }
        sparkMemOverhead = Math.max(sparkMemOverheadLegacy, sparkMemOverhead);

        // Total executor memory includes the memory overhead.
        containerSize = executorMemory + sparkMemOverhead;

        int maxExecutorsSupportedByYARN =
            (memSize / containerSize) > Integer.MAX_VALUE ?
                                        Integer.MAX_VALUE : (int)(memSize / containerSize);

        if (maxExecutorsSupportedByYARN < 1)
            maxExecutorsSupportedByYARN = 1;

        maxExecutorsSupportedByYARN *= numNodes;

        int maxExecutorCoresSupportedByYARN = maxExecutorsSupportedByYARN * executorCores;

        if (dynamicAllocation) {
            if (numSparkExecutors < 1)
                numSparkExecutors = 1;
            numSparkExecutorCores = ((long)numSparkExecutors * executorCores) > Integer.MAX_VALUE ?
                                     Integer.MAX_VALUE : numSparkExecutors * executorCores;
        }

        if (numSparkExecutorCores > maxExecutorCoresSupportedByYARN)
            numSparkExecutorCores = maxExecutorCoresSupportedByYARN;

        return numSparkExecutorCores;
    }

    /**
     * Parse a Spark or Hadoop size parameter value, that may use b, k, m, g, t, p
     * to represent bytes, kilobytes, megabytes, gigabytes, terabytes or petabytes respectively,
     * and return back the corresponding number of bytes.  Valid suffixes can also end
     * with a 'b' : kb, mb, gb, tb, pb.
     * @param sizeString the parameter value string to parse
     * @param defaultValue the default value of the parameter if an invalid
     *                     <code>sizeString</code> was passed.
     * @return The value in bytes of <code>sizeString</code>, or <code>defaultValue</code>
     *         if a <code>sizeString</code> was passed that could not be parsed.
     */
    public static long parseSizeString(String sizeString, long defaultValue, String defaultSuffix) {
        long retVal = defaultValue;
        Pattern sizePattern = Pattern.compile("(^[\\d.]+)([bkmgtp]$)", Pattern.CASE_INSENSITIVE);
        Pattern sizePattern2 = Pattern.compile("(^[\\d.]+)([kmgtp][b]$)", Pattern.CASE_INSENSITIVE);
        sizeString = sizeString.trim();

        // Add a default suffix if none is specified.
        if (sizeString.matches("^.*\\d$"))
            sizeString = sizeString + defaultSuffix;

        Matcher matcher1 = sizePattern.matcher(sizeString);
        Matcher matcher2 = sizePattern2.matcher(sizeString);
        Map<String, Integer> suffixes = new HashMap<>();
        suffixes.put("b", 0);
        suffixes.put("k", 1);
        suffixes.put("m", 2);
        suffixes.put("g", 3);
        suffixes.put("t", 4);
        suffixes.put("p", 5);
        suffixes.put("kb", 1);
        suffixes.put("mb", 2);
        suffixes.put("gb", 3);
        suffixes.put("tb", 4);
        suffixes.put("pb", 5);

        boolean found1 = matcher1.find();
        boolean found2 = matcher2.find();
        Matcher matcher = found1 ? matcher1 : matcher2;

        if (found1 || found2) {
            BigInteger value;
            String digits = matcher.group(1);
            try {
              value = new BigInteger(digits);
            }
            catch (NumberFormatException e) {
              return defaultValue;
            }
            int power = suffixes.get(matcher.group(2).toLowerCase());
            BigInteger multiplicand = BigInteger.valueOf(1024).pow(power);
            value = value.multiply(multiplicand);
            if (value.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0)
              return Long.MAX_VALUE;
            if (value.compareTo(BigInteger.valueOf(0)) < 0)
              return defaultValue;

            retVal = value.longValue();
        }
        else {
            try {
                retVal = Long.parseLong(sizeString);
            }
            catch (NumberFormatException e) {
                return defaultValue;
            }
            if (retVal < 1)
                retVal = defaultValue;
        }
        return retVal;
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

