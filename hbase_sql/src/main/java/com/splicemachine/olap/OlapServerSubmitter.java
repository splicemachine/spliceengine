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

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.access.util.NetworkUtils;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.splicemachine.si.constants.SIConstants.YARN_DEFAULT_QUEUE_NAME;
import static org.apache.zookeeper.KeeperException.Code.NODEEXISTS;
import static org.apache.zookeeper.KeeperException.Code.NONODE;

/**
 * Created by dgomezferro on 29/08/2017.
 */
public class OlapServerSubmitter implements Runnable {

    // Staging directory for any temporary jars or files
    private static final String SPLICE_STAGING = ".spliceStaging";

    private static final String KEYTAB_KEY = "splice.spark.yarn.keytab";
    private static final String PRINCIPAL_KEY = "splice.spark.yarn.principal";
    private static final String HBASE_MASTER_KEYTAB_KEY = "hbase.master.keytab.file";
    private static final String HBASE_MASTER_PRINCIPAL_KEY="hbase.master.kerberos.principal";

    // Staging directory is private! -> rwx--------
    private static final FsPermission STAGING_DIR_PERMISSION = FsPermission.createImmutable(Short.parseShort("700", 8));

    private static final Logger LOG = Logger.getLogger(OlapServerSubmitter.class);
    private final String queueName;
    private volatile boolean stop = false;
    private CountDownLatch stopLatch = new CountDownLatch(1);
    private Path appStagingBaseDir;
    private String amKeytabFileName = null;
    private Thread keepAlive;
    private OlapMessage.KeepAlive keepAlivePrototype;

    private Configuration conf;

    public OlapServerSubmitter(String queueName) {
        this.queueName = queueName;
    }

    @Override
    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "intended")
    public void run() {

        try {
            // Create yarnClient
            conf = HConfiguration.unwrapDelegate();

            initKeepAlivePrototype();

            YarnClient yarnClient = YarnClient.createYarnClient();
            yarnClient.init(conf);
            yarnClient.start();

            SConfiguration sconf = HConfiguration.getConfiguration();

            int maxAttempts = sconf.getOlapServerSubmitAttempts();
            int memory = sconf.getOlapServerMemory();
            int memoryOverhead = sconf.getOlapServerMemoryOverhead();
            int cpuCores = sconf.getOlapVirtualCores();
            int olapPort = sconf.getOlapServerBindPort();
            String stagingDir = sconf.getOlapServerStagingDirectory();
            Map<String, String> yarnQueues = sconf.getOlapServerYarnQueues();

            String sparkYarnQueue = yarnQueues.get(queueName);
            if (sparkYarnQueue == null)
                sparkYarnQueue = YARN_DEFAULT_QUEUE_NAME;

            if (stagingDir != null) {
                this.appStagingBaseDir = new Path(stagingDir);
            } else {
                this.appStagingBaseDir = FileSystem.get(conf).getHomeDirectory();
            }
            String yarnQueue = System.getProperty("splice.olapServer.yarn.queue");
            if (yarnQueue == null) {
                yarnQueue = System.getProperty("splice.spark.yarn.queue", YARN_DEFAULT_QUEUE_NAME);
            }

            for (int i = 0; i<maxAttempts; ++i) {
                try {
                    ApplicationId appId = findApplication(yarnClient);
                    if (appId == null) {
                        appId = submitApplication(yarnClient, memory, memoryOverhead, olapPort, cpuCores, sparkYarnQueue, yarnQueue);
                    }

                    String appName = appId.toString();
                    AtomicBoolean stopAttempt = new AtomicBoolean(false);

                    keepAlive = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            RecoverableZooKeeper zk = ZkUtils.getRecoverableZooKeeper();
                            String root = HConfiguration.getConfiguration().getSpliceRootPath();
                            String keepAlivePath = root + HBaseConfiguration.OLAP_SERVER_PATH + HBaseConfiguration.OLAP_SERVER_KEEP_ALIVE_PATH + "/" + appName;

                            byte[] payload = getKeepAlivePayload();
                            try {
                                // clean up node if it exists
                                ZkUtils.safeDelete(keepAlivePath, -1);
                                zk.create(keepAlivePath, payload, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                            } catch (Exception e) {
                                LOG.error("Couldn't create keepAlive for OlapServer-"+queueName, e);
                                return;
                            }
                            while (!stop && !stopAttempt.get()) {
                                try {
                                    Thread.sleep(30000);
                                    payload = getKeepAlivePayload();
                                    zk.setData(keepAlivePath, payload, -1);
                                } catch (InterruptedException e) {
                                    // We were interrupted, stop keep alive
                                    LOG.warn("Caught interrupted exception, stop OlapServer-"+queueName+" keep alive");
                                    return;
                                } catch (KeeperException e) {
                                    LOG.warn("Caught unexpected exception, retrying OlapServer-"+queueName+" keep alive", e);
                                }
                            }

                            LOG.info("Stopping keepalive for OlapServer-"+queueName);
                        }
                    }, "OlapServer-"+queueName+"-keepalive");
                    keepAlive.setDaemon(true);
                    keepAlive.start();

                    ApplicationReport appReport = yarnClient.getApplicationReport(appId);
                    YarnApplicationState appState = appReport.getYarnApplicationState();
                    while (appState != YarnApplicationState.FINISHED &&
                            appState != YarnApplicationState.KILLED &&
                            appState != YarnApplicationState.FAILED &&
                            !stop) {
                        Thread.sleep(2000);
                        appReport = yarnClient.getApplicationReport(appId);
                        appState = appReport.getYarnApplicationState();
                        if (!appState.equals(YarnApplicationState.RUNNING)) {
                            reportDiagnostics("YARN state for application is " + appState +". Not enough YARN resources?");
                        }
                    }
                    reportDiagnostics(appReport.getDiagnostics());

                    stopAttempt.set(true);

                    if (!stop) {
                        LOG.warn(
                                "Application " + appId + " finished with" +
                                        " state " + appState +
                                        " at " + appReport.getFinishTime() + " : " + appReport.getDiagnostics());
                    } else {
                        LOG.warn("Stop requested, yarn application continues running until timeout");
                        return;
                    }
                } catch (Exception e) {
                    if (!stop) {
                        LOG.error("Exception while submitting Olap Server, retrying in 10s", e);
                        reportDiagnostics(e.getMessage());
                        Thread.sleep(10000);
                    } else {
                        LOG.warn("Exception while submitting Olap Server, but Submitter was already stopped. Stop retrying.", e);
                        break;
                    }
                }
            }
            if (!stop) {
                LOG.error("Maximum number of attempts reached, stopping OlapServer startup");
            } else {
                LOG.info("Submitter is stopped");
            }
        } catch (Exception e) {
            LOG.error("unexpected exception", e);
            throw new RuntimeException(e);
        } finally {
            stopLatch.countDown();
        }

    }

    private void initKeepAlivePrototype() throws Exception {
        DatabaseVersion version = JMXUtils.getLocalMBeanProxy(JMXUtils.SPLICEMACHINE_VERSION, DatabaseVersion.class);
        keepAlivePrototype = OlapMessage.KeepAlive.newBuilder()
                .setMajor(version.getMajorVersionNumber())
                .setMinor(version.getMinorVersionNumber())
                .setPatch(version.getPatchVersionNumber())
                .setSprint(version.getSprintVersionNumber())
                .setImplementation(version.getImplementationVersion()).buildPartial();
    }

    private byte[] getKeepAlivePayload() {
        OlapMessage.KeepAlive message = OlapMessage.KeepAlive.newBuilder(keepAlivePrototype)
                .setTime(System.currentTimeMillis())
                .build();
        return message.toByteArray();
    }

    private ApplicationId findApplication(YarnClient yarnClient) throws IOException, YarnException {
        List<ApplicationReport> reports = yarnClient.getApplications(Collections.singleton("YARN"),
                EnumSet.of(YarnApplicationState.RUNNING, YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING));
        for (ApplicationReport ar : reports) {
            if (ar.getName().equals("OlapServer-"+queueName) && ar.getUser().equals(UserGroupInformation.getCurrentUser().getUserName())) {
                LOG.info("Found existing application: " + ar);
                return ar.getApplicationId();
            }
        }
        return null;
    }

    private void reportDiagnostics(String diagnostics) {
        try {
            RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
            String root = HConfiguration.getConfiguration().getSpliceRootPath();

            String diagnosticsPath = root + HBaseConfiguration.OLAP_SERVER_PATH + HBaseConfiguration.OLAP_SERVER_DIAGNOSTICS_PATH + "/" + queueName;

            if (rzk.exists(diagnosticsPath, false) != null) {
                rzk.setData(diagnosticsPath, Bytes.toBytes(diagnostics), -1);
            } else {
                rzk.create(diagnosticsPath, Bytes.toBytes(diagnostics), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            LOG.error("Exception while trying to report diagnostics", e);
            // ignore this exception during error reporting
        }
    }

    private ApplicationId submitApplication(YarnClient yarnClient, int memory, int memoryOverhead,
                                            int olapPort, int cpuCores, String sparkYarnQueue, String yarnQueue)
            throws KeeperException, InterruptedException, IOException, URISyntaxException, YarnException {
        // Clear ZooKeeper path
        clearZookeeper(ZkUtils.getRecoverableZooKeeper(), queueName);

        // Create application via yarnClient
        YarnClientApplication app = yarnClient.createApplication();

        GetNewApplicationResponse newAppResponse = app.getNewApplicationResponse();
        ApplicationId appId = newAppResponse.getApplicationId();

        Path appStagingDirPath = new Path(appStagingBaseDir, getAppStagingDir(appId));
        // Create staging dir
        FileSystem fs = appStagingDirPath.getFileSystem(conf);
        FileSystem.mkdirs(fs, appStagingDirPath, new FsPermission(STAGING_DIR_PERMISSION));

        String keytab = getKeytab();
        Map<String, LocalResource> localResources = new HashMap<>();
        if (keytab != null) {
            LOG.info(KEYTAB_KEY + " is set, adding it to local resources");
            String trimmedPath = keytab.trim();
            URI localURI = resolveURI(trimmedPath);
            Path srcPath = getQualifiedLocalPath(localURI, conf);

            String name = srcPath.getName();
            FileSystem srcFs = srcPath.getFileSystem(conf);
            FileUtil.copy(srcFs, srcPath, fs, appStagingDirPath, false, conf);
            amKeytabFileName = name;

            Path keytabPath = new Path(appStagingDirPath, name);
            FileStatus destStatus = fs.getFileStatus(keytabPath);
            LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
            amJarRsrc.setType(LocalResourceType.FILE);
            amJarRsrc.setVisibility(LocalResourceVisibility.PRIVATE);
            amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(keytabPath));
            amJarRsrc.setTimestamp(destStatus.getModificationTime());
            amJarRsrc.setSize(destStatus.getLen());

            localResources.put(name, amJarRsrc);
        }

        SConfiguration config = SIDriver.driver().getConfiguration();
        String log4jConfig = config.getOlapLog4jConfig();
        String log4jDefault = LogManager.DEFAULT_CONFIGURATION_FILE;
        URI log4jURI = null;
        if (log4jConfig != null) {
            File log4jFile = new File(new URI(log4jConfig).getPath());
            if (log4jFile.exists()) {
                log4jURI = log4jFile.toURI();
            } else {
                URL log4jURL = this.getClass().getResource(log4jConfig);
                if (log4jURL != null) {
                    log4jURI = log4jURL.toURI();
                }
            }
        }
        if (log4jURI != null) {
            LOG.info("Got log4j config for OLAP server: " + log4jURI);
            Path log4jPath = getQualifiedLocalPath(log4jURI, conf);
            FileSystem srcFs = log4jPath.getFileSystem(conf);
            Path log4jHPath = new Path(appStagingDirPath, log4jDefault);
            FileUtil.copy(srcFs, log4jPath, fs, log4jHPath, false, conf);
            LOG.info("Copy log4j config file from " + log4jURI + " to "
                    + log4jHPath.toUri());
            FileStatus destStatus = fs.getFileStatus(log4jHPath);
            LocalResource log4jRsrc = Records.newRecord(LocalResource.class);
            log4jRsrc.setType(LocalResourceType.FILE);
            log4jRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
            log4jRsrc.setResource(ConverterUtils.getYarnUrlFromPath(log4jHPath));
            log4jRsrc.setTimestamp(destStatus.getModificationTime());
            log4jRsrc.setSize(destStatus.getLen());
            localResources.put(log4jDefault, log4jRsrc);
        } else {
            LOG.warn("Log4j config for OLAP server not found.");
        }

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer =
                Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(
                Collections.singletonList(prepareCommands(
                        "$JAVA_HOME/bin/java",
                        " -Xmx" + memory + "M " + outOfMemoryErrorArgument() + " " +
                                OlapServerMaster.class.getCanonicalName() +
                                " " + olapPort + " " + queueName + " YARN " + appId +
                                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr",
                        sparkYarnQueue
                ))
        );

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        // add log4j.properties to the classpath
        if (log4jURI != null) {
            addPathToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                    ApplicationConstants.Environment.PWD.$() + File.separator);
        }
        setupAppMasterEnv(appMasterEnv, conf);
        amContainer.setEnvironment(appMasterEnv);
        amContainer.setLocalResources(localResources);

        // Setup security tokens
        if (UserGroupInformation.isSecurityEnabled()) {
            Credentials credentials = new Credentials();
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }

            final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null) {
                for (Token<?> token : tokens) {
                    LOG.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(memory + memoryOverhead);
        capability.setVirtualCores(cpuCores);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext =
                app.getApplicationSubmissionContext();
        String appName = "OlapServer-"+queueName;
        appContext.setApplicationName(appName); // application name
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue(yarnQueue);
        appContext.setMaxAppAttempts(1);
        appContext.setAttemptFailuresValidityInterval(10000);

        // Submit application
        LOG.info("Submitting YARN application " + appId);

        yarnClient.submitApplication(appContext);
        return appId;
    }

    /**
     * If security is enabled, but the user did not specify a keytab, use hbase master's keytab
     * @return
     */
    private String getKeytab() {
        String keytab = System.getProperty(KEYTAB_KEY);
        if (keytab == null && UserGroupInformation.isSecurityEnabled()) {
            keytab = HConfiguration.unwrapDelegate().get(HBASE_MASTER_KEYTAB_KEY);
        }
        return keytab;
    }

    /** Create ZK path if it doesn't exist. Returns whether or not the path was created */
    private boolean createPath(RecoverableZooKeeper rzk, String path) throws KeeperException, InterruptedException {
        try {
            rzk.create(path, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return true;
        } catch (KeeperException e) {
            if (e.code().equals(NODEEXISTS)) {
                return false;
            }
            else throw e;
        }
    }

    private void clearZookeeper(RecoverableZooKeeper rzk, String queueName) throws InterruptedException, KeeperException {
        String root = HConfiguration.getConfiguration().getSpliceRootPath();

        try {
            String masterPath = root + HBaseConfiguration.OLAP_SERVER_PATH;

            // Create Path structure, if we can create it it was empty and we don't have to clear it
            createPath(rzk, masterPath);
            createPath(rzk, masterPath + HBaseConfiguration.OLAP_SERVER_KEEP_ALIVE_PATH);
            createPath(rzk, masterPath + HBaseConfiguration.OLAP_SERVER_DIAGNOSTICS_PATH);
            if (createPath(rzk, masterPath + HBaseConfiguration.OLAP_SERVER_QUEUE_PATH))
                return;

            List<String> servers = rzk.getChildren(masterPath + HBaseConfiguration.OLAP_SERVER_QUEUE_PATH, false);

            // clear existing queue znodes
            servers.stream()
                    .map(OlapServerZNode::parseFrom)
                    .filter(n -> n.getQueueName().equals(queueName))
                    .map(OlapServerZNode::toZNode).forEach(
                    n -> {
                        try {
                            rzk.delete(masterPath + HBaseConfiguration.OLAP_SERVER_QUEUE_PATH + "/" + n, -1);
                        } catch (KeeperException e) {
                            if (e.code().equals(NONODE)) {
                                // ignore, it just got deleted
                            }
                            else throw new RuntimeException(e);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );

            // clear leader election znodes
            try {
                ZkUtils.recursiveDelete(masterPath + HBaseConfiguration.OLAP_SERVER_LEADER_ELECTION_PATH + "/" + queueName);
            } catch (Exception e) {
                KeeperException ke = null;
                if (e instanceof KeeperException) {
                    ke = (KeeperException) e;
                } else if (e.getCause() instanceof KeeperException) {
                    ke = (KeeperException) e.getCause();
                }
                if (ke != null && ke.code().equals(NONODE)) {
                    // ignore, it just got deleted
                }
                else throw new RuntimeException(e);
            }
        } catch (Exception e) {
            LOG.error("Couldn't clear OlapServer zookeeper node due to unexpected exception", e);
            throw e;
        }
    }


    /**
     * Kill if OOM is raised - leverage yarn's failure handling to cause rescheduling.
     * Not killing the task leaves various aspects of the executor and (to some extent) the jvm in
     * an inconsistent state.
     * TODO: If the OOM is not recoverable by rescheduling it on different node, then do
     * 'something' to fail job ... akin to blacklisting trackers in mapred ?
     *
     * The handler if an OOM Exception is thrown by the JVM must be configured on Windows
     * differently: the 'taskkill' command should be used, whereas Unix-based systems use 'kill'.
     *
     * As the JVM interprets both %p and %%p as the same, we can use either of them. However,
     * some tests on Windows computers suggest, that the JVM only accepts '%%p'.
     *
     * Furthermore, the behavior of the character '%' on the Windows command line differs from
     * the behavior of '%' in a .cmd file: it gets interpreted as an incomplete environment
     * variable. Windows .cmd files escape a '%' by '%%'. Thus, the correct way of writing
     * '%%p' in an escaped way is '%%%%p'.
     */
    private String outOfMemoryErrorArgument() {
        if (SystemUtils.IS_OS_WINDOWS) {
            return quoteForBatchScript("-XX:OnOutOfMemoryError=taskkill /F /PID %%%%p");
        } else {
            return "-XX:OnOutOfMemoryError='kill %p'";
        }
    }

    /**
     * Quote a command argument for a command to be run by a Windows batch script, if the argument
     * needs quoting. Arguments only seem to need quotes in batch scripts if they have certain
     * special characters, some of which need extra (and different) escaping.
     *
     *  For example:
     *    original single argument: ab="cde fgh"
     *    quoted: "ab^=""cde fgh"""
     *
     * Copied from Spark's CommandBuilderUtils
     */
    static String quoteForBatchScript(String arg) {

        boolean needsQuotes = false;
        for (int i = 0; i < arg.length(); i++) {
            int c = arg.codePointAt(i);
            if (Character.isWhitespace(c) || c == '"' || c == '=' || c == ',' || c == ';') {
                needsQuotes = true;
                break;
            }
        }
        if (!needsQuotes) {
            return arg;
        }
        StringBuilder quoted = new StringBuilder();
        quoted.append("\"");
        for (int i = 0; i < arg.length(); i++) {
            int cp = arg.codePointAt(i);
            switch (cp) {
                case '"':
                    quoted.append('"');
                    break;

                default:
                    break;
            }
            quoted.appendCodePoint(cp);
        }
        if (arg.codePointAt(arg.length() - 1) == '\\') {
            quoted.append("\\");
        }
        quoted.append("\"");
        return quoted.toString();
    }


    private String prepareCommands(String exec, String parameters, String sparkYarnQueue) throws IOException {
        StringBuilder result = new StringBuilder();
        result.append(exec);
        for (Object sysPropertyKey : System.getProperties().keySet()) {
            String spsPropertyName = (String) sysPropertyKey;
            if (spsPropertyName.contains("spark.yarn.queue"))
                continue; // we'll set the appropriate yarn queue later
            if (spsPropertyName.startsWith("splice.spark") || spsPropertyName.startsWith("spark")) {
                if (spsPropertyName.equals(KEYTAB_KEY)) {
                    LOG.info(KEYTAB_KEY + " is set, substituting it for " + amKeytabFileName);
                    result.append(' ').append("-D"+spsPropertyName+"="+amKeytabFileName);
                    continue;
                }
                String sysPropertyValue = System.getProperty(spsPropertyName).replace('\n', ' ');
                if (sysPropertyValue != null) {
                    result.append(' ').append("-D"+spsPropertyName+"=\\\""+sysPropertyValue+"\\\"");
                }
            }
        }
        result.append(' ').append("-Dspark.yarn.queue=\\\""+sparkYarnQueue+"\\\"");
        result.append(' ').append("-Dsplice.spark.app.name=\\\"SpliceMachine-"+queueName+"\\\"");
        // If user does not specify a kerberos keytab or principal, use HBase master's.
        if (UserGroupInformation.isSecurityEnabled()) {
            Configuration configuration = HConfiguration.unwrapDelegate();
            String principal = System.getProperty(PRINCIPAL_KEY);
            String keytab = System.getProperty(KEYTAB_KEY);
            if (principal == null || keytab == null) {
                principal = configuration.get(HBASE_MASTER_PRINCIPAL_KEY);
                String hostname = NetworkUtils.getHostname(HConfiguration.getConfiguration());
                principal = SecurityUtil.getServerPrincipal(principal, hostname);
                SpliceLogUtils.info(LOG, "User did not specify principal or keytab, use default principal=%s, keytab=%s", principal, amKeytabFileName);
                result.append(' ').append("-D"+PRINCIPAL_KEY+"="+principal);
                result.append(' ').append("-D"+KEYTAB_KEY+"="+amKeytabFileName);
            }
        }
        String extraOptions = System.getProperty("splice.olapServer.extraJavaOptions");
        if (extraOptions != null) {
            for (String option : extraOptions.split("\\s+")) {
                result.append(' ').append(option);
            }
        }
        result.append(' ').append(parameters);
        String command = result.toString();
        LOG.info("OlapServer command: " + command);
        return command;
    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv, Configuration conf) {

        String sparkJars = System.getProperty("splice.spark.yarn.jars");
        if (sparkJars != null) {
            addPathToEnvironment(appMasterEnv,
                    ApplicationConstants.Environment.CLASSPATH.name(), sparkJars);
        }

        String classpath = System.getProperty("splice.olapServer.classpath");
        if (classpath != null) {
            addPathToEnvironment(appMasterEnv,
                    ApplicationConstants.Environment.CLASSPATH.name(), classpath);
        }

        addPathToEnvironment(appMasterEnv,
                ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*");
        addPathToEnvironment(appMasterEnv,
                ApplicationConstants.Environment.CLASSPATH.name(), System.getProperty("splice.spark.executor.extraClassPath"));

        addPathToEnvironment(appMasterEnv,
                ApplicationConstants.Environment.CLASSPATH.name(), expandEnvironment(ApplicationConstants.Environment.PWD));

        for (String path : getYarnAppClasspath(conf)) {
            addPathToEnvironment(appMasterEnv,
                    ApplicationConstants.Environment.CLASSPATH.name(), path.trim());
        }
        for (String path : getMRAppClasspath(conf)) {
            addPathToEnvironment(appMasterEnv,
                    ApplicationConstants.Environment.CLASSPATH.name(), path.trim());
        }

        LOG.debug("CLASSPATH: " + appMasterEnv.get(ApplicationConstants.Environment.CLASSPATH.name()));

    }
    
    public String[] getYarnAppClasspath(Configuration conf ) {
        String[] strings = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
        if (strings != null)
            return strings;
        return YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH;
    }

    public String[] getMRAppClasspath(Configuration conf ) {
        String[] strings = conf.getStrings("mapreduce.application.classpath");
        if (strings != null)
            return strings;
        return StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH);
    }

    private String expandEnvironment(ApplicationConstants.Environment pwd) {
        return pwd.$$();
    }

    private void addPathToEnvironment(Map<String, String> env, String key, String value) {
        String newValue = value;
        if (env.containsKey(key)) {
            newValue = env.get(key) + ApplicationConstants.CLASS_PATH_SEPARATOR  + value;
        }
        env.put(key, newValue);
    }

    public void stop() {
        LOG.warn("Stopping OlapServerSubmitter");
        stop = true;
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for OlapServerSubmitter to finish", e);
        }
    }
    /**
     * Return the path to the given application's staging directory.
     */
    private String getAppStagingDir(ApplicationId appId) {
        return buildPath(SPLICE_STAGING, appId.toString());
    }

    private String buildPath(String... components) {
        return String.join(Path.SEPARATOR, components);
    }

    private URI resolveURI(String path) {
        try {
            URI uri = new URI(path);
            if (uri.getScheme() != null) {
                return uri;
            }
        } catch (URISyntaxException e) {
            // ignore
        }
        return new File(path).getAbsoluteFile().toURI();
    }

    private Path getQualifiedLocalPath(URI localURI, Configuration hadoopConf) throws IOException, URISyntaxException {
        URI qualifiedURI;
        if (localURI.getScheme() == null) {
            // If not specified, assume this is in the local filesystem to keep the behavior
            // consistent with that of Hadoop
            qualifiedURI = new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString());
        } else {
            qualifiedURI = localURI;
        }
        return new Path(qualifiedURI);
    }
}
