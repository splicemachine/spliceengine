package com.splicemachine.yarn.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * Test connecting and running against an existing (already running) YARN cluster.
 *
 */
public class BareYarnTest {
    // TODO: JC - how to connect to server w/o knowing port (MiniYARNCluster ignores ports in yarn-site.xml

    private static Configuration conf = null;
    private static YarnClient yarnClient = null;

//    @BeforeClass
    public static void beforeClass() throws Exception {
        URL configURL = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
        if (configURL == null) {
            throw new RuntimeException("Could not find 'yarn-site.xml' file in classpath");
        }

        conf = new YarnConfiguration();
        conf.set("yarn.application.classpath", new File(configURL.getPath()).getParent());


        // start rm client
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

    }

//    @AfterClass
    public static void tearDown() {
        if (yarnClient != null && yarnClient.getServiceState() == Service.STATE.STARTED) {
            yarnClient.stop();
        }
    }

//    @Test (timeout=60000)
    public void testAMRMClientMatchingFitInferredRack() throws YarnException, IOException {
        // submit new app
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
//        RMAppAttempt appAttempt;
//        while (true) {
//            ApplicationReport appReport = yarnClient.getApplicationReport(appId);
//            if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
//                ApplicationAttemptId attemptId = appReport.getCurrentApplicationAttemptId();
//                appAttempt =
//                    testYarnParticipant.getYarnCluster().getResourceManager().getRMContext().getRMApps()
//                                       .get(attemptId.getApplicationId()).getCurrentAppAttempt();
//                while (true) {
//                    if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
//                        break;
//                    }
//                }
//                break;
//            }
//        }
//        // Just dig into the ResourceManager and get the AMRMToken just for the sake
//        // of testing.
//        UserGroupInformation.setLoginUser(UserGroupInformation
//                                              .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
//        UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
    }

}
