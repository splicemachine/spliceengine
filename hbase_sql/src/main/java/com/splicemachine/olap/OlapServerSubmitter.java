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

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;
import org.apache.spark.util.ShutdownHookManager;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by dgomezferro on 29/08/2017.
 */
public class OlapServerSubmitter implements Runnable {

    // Staging directory for any temporary jars or files
    private static final String SPLICE_STAGING = ".spliceStaging";


    // Staging directory is private! -> rwx--------
    private static final FsPermission STAGING_DIR_PERMISSION = FsPermission.createImmutable(Short.parseShort("700", 8));

    private static final Logger LOG = Logger.getLogger(OlapServerSubmitter.class);
    private final ServerName serverName;
    private volatile boolean stop = false;
    private CountDownLatch stopLatch = new CountDownLatch(1);
    private Path appStagingBaseDir;

    private Configuration conf;

    public OlapServerSubmitter(ServerName serverName) {
        this.serverName = serverName;
    }

    public void run() {

        try {
            // Create yarnClient
            conf = HConfiguration.unwrapDelegate();
            this.appStagingBaseDir = FileSystem.get(conf).getHomeDirectory();

            YarnClient yarnClient = YarnClient.createYarnClient();
            yarnClient.init(conf);
            yarnClient.start();

            SConfiguration sconf = HConfiguration.getConfiguration();

            int maxAttempts = sconf.getOlapServerSubmitAttempts();
            int memory = sconf.getOlapServerMemory();
            int memoryOverhead = sconf.getOlapServerMemoryOverhead();
            int cpuCores = sconf.getOlapVirtualCores();
            int olapPort = sconf.getOlapServerBindPort();

            for (int i = 0; i<maxAttempts; ++i) {
                // Create application via yarnClient
                YarnClientApplication app = yarnClient.createApplication();

                // Set up the container launch context for the application master
                ContainerLaunchContext amContainer =
                        Records.newRecord(ContainerLaunchContext.class);
                amContainer.setCommands(
                        Collections.singletonList(prepareCommands(
                                "$JAVA_HOME/bin/java",
                                        " -Xmx" + memory + "M " + outOfMemoryErrorArgument() + " " +
                                        OlapServerMaster.class.getCanonicalName() +
                                        " " + serverName.toString() + " " + olapPort +
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ))
                );

                // Setup CLASSPATH for ApplicationMaster
                Map<String, String> appMasterEnv = new HashMap<String, String>();
                setupAppMasterEnv(appMasterEnv, conf);
                amContainer.setEnvironment(appMasterEnv);


                // Set up resource type requirements for ApplicationMaster
                Resource capability = Records.newRecord(Resource.class);
                capability.setMemory(memory + memoryOverhead);
                capability.setVirtualCores(cpuCores);

                // Finally, set-up ApplicationSubmissionContext for the application
                ApplicationSubmissionContext appContext =
                        app.getApplicationSubmissionContext();
                appContext.setApplicationName("OlapServer"); // application name
                appContext.setResource(capability);
                appContext.setQueue("default"); // queue
                appContext.setMaxAppAttempts(1);
                appContext.setAttemptFailuresValidityInterval(10000);

                // Submit application
                ApplicationId appId = appContext.getApplicationId();
                LOG.info("Submitting YARN application " + appId);

                Path appStagingDirPath = new Path(appStagingBaseDir, getAppStagingDir(appId));

                yarnClient.submitApplication(appContext);
                Object hookReference = ShutdownHookManager.addShutdownHook(0, new AbstractFunction0<BoxedUnit>() {
                    @Override
                    public BoxedUnit apply() {
                        try {
                            stop();
                            yarnClient.killApplication(appId);
                        } catch (Exception e) {
                            LOG.error("Exception while running shutdown hook", e);
                        }
                        return null;
                    }
                });

                ApplicationReport appReport = yarnClient.getApplicationReport(appId);
                YarnApplicationState appState = appReport.getYarnApplicationState();
                while (appState != YarnApplicationState.FINISHED &&
                        appState != YarnApplicationState.KILLED &&
                        appState != YarnApplicationState.FAILED &&
                        !stop) {
                    Thread.sleep(1000);
                    appReport = yarnClient.getApplicationReport(appId);
                    appState = appReport.getYarnApplicationState();
                }

                ShutdownHookManager.removeShutdownHook(hookReference);
                if (!stop) {
                    LOG.warn(
                            "Application " + appId + " finished with" +
                                    " state " + appState +
                                    " at " + appReport.getFinishTime() + " : " + appReport.getDiagnostics());
                } else {
                    LOG.warn("Stop requested, shutting down yarn application");
                    yarnClient.killApplication(appId);
                    return;
                }
            }

            LOG.error("Maximum number of attempts reached, stopping OlapServer startup");
        } catch (Exception e) {
            LOG.error("unexpected exception", e);
        } finally {
            stopLatch.countDown();
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


    private String prepareCommands(String exec, String parameters) {
        StringBuilder result = new StringBuilder();
        result.append(exec);
        for (Object sysPropertyKey : System.getProperties().keySet()) {
            String spsPropertyName = (String) sysPropertyKey;
            if (spsPropertyName.startsWith("splice.spark") || spsPropertyName.startsWith("spark")) {
                String sysPropertyValue = System.getProperty(spsPropertyName).replace('\n', ' ');
                if (sysPropertyValue != null) {
                    result.append(' ').append("-D"+spsPropertyName+"=\\\""+sysPropertyValue+"\\\"");
                }
            }
        }
        String extraOptions = System.getProperty("splice.olapServer.extraJavaOptions");
        if (extraOptions != null) {
            for (String option : extraOptions.split("\\s+")) {
                result.append(' ').append(option);
            }
        }
        result.append(' ').append(parameters);
        return result.toString();
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

        LOG.debug("CLASSPATH: + " + appMasterEnv.get(ApplicationConstants.Environment.CLASSPATH.name()));

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
}
