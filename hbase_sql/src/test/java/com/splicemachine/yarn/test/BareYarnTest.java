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

package com.splicemachine.yarn.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.test.SpliceTestYarnPlatform;

/**
 * Test connecting and running Yarn client against a YARN cluster using only the yarn-site.xml,
 * that is, don't get the config from the running server, get the config from yarn-site.xml to
 * know where to connect to the server.
 *
 */
public class BareYarnTest {
    private static SpliceTestYarnPlatform yarnPlatform = null;
    private static YarnClient yarnClient = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // start yarn server
        yarnPlatform = new SpliceTestYarnPlatform();
        yarnPlatform.start(SpliceTestYarnPlatform.DEFAULT_NODE_COUNT);

        URL configURL = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
        if (configURL == null) {
            throw new RuntimeException("Could not find 'yarn-site.xml' file in classpath");
        }

        Configuration conf = new YarnConfiguration();
        conf.set("yarn.application.classpath", new File(configURL.getPath()).getParent());


        // start rm client
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (yarnClient != null && yarnClient.getServiceState() == Service.STATE.STARTED) {
            yarnClient.stop();
        }

        // stop yarn server
        if (yarnPlatform != null &&
            yarnPlatform.getYarnCluster() != null &&
            yarnPlatform.getYarnCluster().getServiceState() == Service.STATE.STARTED) {
            yarnPlatform.stop();
        }
    }

    /**
     * All we really need to do here is to create a yarn client, configure it using the same
     * yarn-site.xml as was used by the server to start up.
     * @throws YarnException
     * @throws IOException
     */
    @Test(timeout=60000)   @Ignore("Broken by dependency change")
    public void testAMRMClientMatchingFitInferredRack() throws YarnException, IOException {
        // create, submit new app
        ApplicationSubmissionContext appContext =
            yarnClient.createApplication().getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        // set the application name
        appContext.setApplicationName("Test");
        // Set the priority for the application master
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);
        appContext.setPriority(pri);
        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue("default");
        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer =
            BuilderUtils.newContainerLaunchContext(
                Collections.<String, LocalResource>emptyMap(),
                new HashMap<String, String>(), Arrays.asList("sleep", "100"),
                new HashMap<String, ByteBuffer>(), null,
                new HashMap<ApplicationAccessType, String>());
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(Resource.newInstance(1024, 1));
        // Create the request to send to the applications manager
        SubmitApplicationRequest appRequest = Records
            .newRecord(SubmitApplicationRequest.class);
        appRequest.setApplicationSubmissionContext(appContext);
        // Submit the application to the applications manager
        yarnClient.submitApplication(appContext);

        // wait for app to start
        RMAppAttempt appAttempt;
        while (true) {
            ApplicationReport appReport = yarnClient.getApplicationReport(appId);
            if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
                ApplicationAttemptId attemptId = appReport.getCurrentApplicationAttemptId();
                appAttempt =
                    yarnPlatform.getResourceManager().getRMContext().getRMApps()
                                       .get(attemptId.getApplicationId()).getCurrentAppAttempt();
                while (true) {
                    if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
                        break;
                    }
                }
                break;
            }
        }
        // Just dig into the ResourceManager and get the AMRMToken just for the sake
        // of testing.
        UserGroupInformation.setLoginUser(UserGroupInformation
                                              .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
        UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
    }

}
