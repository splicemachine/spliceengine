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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;
import org.apache.spark.util.ShutdownHookManager;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by dgomezferro on 29/08/2017.
 */
public class OlapServerSubmitter implements Runnable {
    private static final Logger LOG = Logger.getLogger(OlapServerSubmitter.class);
    private final ServerName serverName;
    private volatile boolean stop = false;

    private Configuration conf;

    public OlapServerSubmitter(ServerName serverName) {
        this.serverName = serverName;
    }

    public void run() {

        try {
            // Create yarnClient
            conf = HConfiguration.unwrapDelegate();
            YarnClient yarnClient = YarnClient.createYarnClient();
            yarnClient.init(conf);
            yarnClient.start();

            while (true) {
                // Create application via yarnClient
                YarnClientApplication app = yarnClient.createApplication();

                // Set up the container launch context for the application master
                ContainerLaunchContext amContainer =
                        Records.newRecord(ContainerLaunchContext.class);
                amContainer.setCommands(
                        Collections.singletonList(prepareCommands(
                                "$JAVA_HOME/bin/java",
                                //" -cp " + System.getProperty("splice.spark.executor.extraClassPath") +
                                        " -Xmx1024M " +
                                        OlapServerMaster.class.getCanonicalName() +
                                        " " + serverName.toString() +
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ))
                );

                // Setup CLASSPATH for ApplicationMaster
                Map<String, String> appMasterEnv = new HashMap<String, String>();
                setupAppMasterEnv(appMasterEnv);
                amContainer.setEnvironment(appMasterEnv);


                // Set up resource type requirements for ApplicationMaster
                Resource capability = Records.newRecord(Resource.class);
                capability.setMemory(1536);
                capability.setVirtualCores(1);

                // Finally, set-up ApplicationSubmissionContext for the application
                ApplicationSubmissionContext appContext =
                        app.getApplicationSubmissionContext();
                appContext.setApplicationName("OlapServer"); // application name
                appContext.setAMContainerSpec(amContainer);
                appContext.setResource(capability);
                appContext.setQueue("default"); // queue
                appContext.setMaxAppAttempts(1);
                appContext.setAttemptFailuresValidityInterval(10000);

                // Submit application
                ApplicationId appId = appContext.getApplicationId();
                System.out.println("Submitting application " + appId);
                yarnClient.submitApplication(appContext);
                Object hookReference = ShutdownHookManager.addShutdownHook(0, new AbstractFunction0<BoxedUnit>() {
                    @Override
                    public BoxedUnit apply() {
                        try {
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
        } catch (Exception e) {
            LOG.error("unexpected exception", e);
        }

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

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        String classpath = System.getProperty("splice.olapServer.classpath");
            addPathToEnvironment(appMasterEnv,
                    ApplicationConstants.Environment.CLASSPATH.name(),
                    ApplicationConstants.Environment.PWD.$() + File.separator + "*");
            addPathToEnvironment(appMasterEnv,
                    ApplicationConstants.Environment.CLASSPATH.name(), System.getProperty("splice.spark.executor.extraClassPath"));
        if (classpath != null) {
            addPathToEnvironment(appMasterEnv,
                    ApplicationConstants.Environment.CLASSPATH.name(), classpath);
        }
    }

    private void addPathToEnvironment(Map<String, String> env, String key, String value) {
        String newValue = value;
        if (env.containsKey(key)) {
            newValue = env.get(key) + ApplicationConstants.CLASS_PATH_SEPARATOR  + value;
        }
        env.put(key, newValue);
    }

    public void stop() {
        stop = true;
    }

}
