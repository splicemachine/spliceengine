/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 *  version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNClusterSplice;
import org.apache.hadoop.yarn.server.Utils;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.log4j.Logger;
import splice.aws.com.amazonaws.auth.AWSCredentials;
import splice.aws.com.amazonaws.auth.AWSCredentialsProvider;
import splice.aws.com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import splice.aws.com.amazonaws.auth.profile.ProfileCredentialsProvider;

/**
 * Starts Yarn server
 */
public class SpliceTestYarnPlatform {
    public static final int DEFAULT_HEARTBEAT_INTERVAL = 100;
    public static int DEFAULT_NODE_COUNT = 1;

    private static final Logger LOG = Logger.getLogger(SpliceTestYarnPlatform.class);

    private URL yarnSiteConfigURL = null;
    private CompositeService yarnCluster = null;
    private Configuration conf = null;
    private String keytab;
    private boolean secure;

    public SpliceTestYarnPlatform(String classPathRoot, boolean secure) {
        this.secure = secure;
        // for testing
        try {
            configForTesting(classPathRoot);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Error trying to config.", e);
        }
    }

    public static void main(String[] args) throws Exception {
        String classPathRoot;
        boolean secure;
        int nodeCount = DEFAULT_NODE_COUNT;
        if (args != null && args.length > 1) {
            classPathRoot = args[0];
            secure = Boolean.parseBoolean(args[1]);
        } else {
            throw new RuntimeException("Use main method for testing with splice yarn client. First arg is required " +
                                           "is the path to the root of the server classpath. This is required so that " +
                                           "splice clients can find the server configuration (yarn-site.xml) in order " +
                                           "to connect.");
        }
        if (args.length > 2) {
            nodeCount = Integer.parseInt(args[2]);
        }

        SpliceTestYarnPlatform yarnParticipant = new SpliceTestYarnPlatform(classPathRoot, secure);
        yarnParticipant.configForSplice(classPathRoot);
        LOG.info("Yarn -- > class " + yarnParticipant.getConfig().get("yarn.nodemanager.container-executor.class"));
        yarnParticipant.start(nodeCount);
    }

    public Configuration getConfig() {
        return conf;
    }

    public CompositeService getYarnCluster() {
        return yarnCluster;
    }

    public void stop() {
        if (yarnCluster != null && yarnCluster.getServiceState() == Service.STATE.STARTED) {
            yarnCluster.stop();
        }
    }
    // todo: this is the same as SpliceTestPlatform.ConfigureS3( config ),
    // but I didn't manage to get dependencies right
    private static void configureS3(Configuration config)
    {
        // AWS Credentials for test
        AWSCredentialsProvider credentialproviders[] = {
                new EnvironmentVariableCredentialsProvider(), // first try env AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
                new ProfileCredentialsProvider()              // second try from $HOME/.aws/credentials (aws cli store)
        };
        for( AWSCredentialsProvider provider : credentialproviders )
        {
            try {
                // can throw SdkClientException if env is not set or can't parse .aws/credentials
                // or IllegalArgumentException if .aws/credentials is not there
                AWSCredentials cred = provider.getCredentials();
                config.set("presto.s3.access-key", cred.getAWSAccessKeyId());
                config.set("presto.s3.secret-key", cred.getAWSSecretKey());
                config.set("fs.s3a.access.key", cred.getAWSAccessKeyId());
                config.set("fs.s3a.secret.key", cred.getAWSSecretKey());
                config.set("fs.s3a.awsAccessKeyId", cred.getAWSAccessKeyId());
                config.set("fs.s3a.awsSecretAccessKey", cred.getAWSSecretKey());
                break;
            } catch( Exception e )
            {
                continue;
            }
        }
        config.set("fs.s3a.impl","com.splicemachine.fs.s3.PrestoS3FileSystem");
    }

    public void start(int nodeCount) throws Exception {
        if (yarnCluster == null) {
            LOG.info("Starting up YARN cluster with "+nodeCount+" nodes. Server yarn-site.xml is: "+yarnSiteConfigURL);

            UserGroupInformation ugi;
            if (secure)
                ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("yarn/example.com@EXAMPLE.COM", keytab);
            else
                ugi = UserGroupInformation.createRemoteUser("yarn");
            
            UserGroupInformation.setLoginUser(ugi);

            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    yarnCluster = new MiniYARNClusterSplice(SpliceTestYarnPlatform.class.getSimpleName(), nodeCount, 1, 1);
                    yarnCluster.init(conf);
                    yarnCluster.start();
                    return null;
                }

            });


            NodeManager nm = getNodeManager();
            waitForNMToRegister(nm);

            // save the server config to classpath so yarn clients can read it
            Configuration yarnClusterConfig = yarnCluster.getConfig();
            yarnClusterConfig.set("yarn.application.classpath", new File(yarnSiteConfigURL.getPath()).getParent());

            configureS3( yarnClusterConfig );

            //write the document to a buffer (not directly to the file, as that
            //can cause the file being written to get read -which will then fail.
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            yarnClusterConfig.writeXml(bytesOut);
            bytesOut.close();
            //write the bytes to the file in the classpath
            OutputStream os = new FileOutputStream(new File(yarnSiteConfigURL.getPath()));
            os.write(bytesOut.toByteArray());
            os.close();
        }
        LOG.info("YARN cluster started.");
    }

    private void configForSplice(String classPathRoot) throws URISyntaxException, MalformedURLException {
        LOG.info("Classpath root: "+classPathRoot);
        if (classPathRoot == null || classPathRoot.isEmpty()) {
            throw new RuntimeException("Can't find path to classpath root: "+classPathRoot);
        }
        File cpRootFile = new File(classPathRoot);
        if (! cpRootFile.exists()) {
            throw new RuntimeException("Can't find path to classpath root: "+classPathRoot);
        }
        cpRootFile = new File(classPathRoot, "/yarn-site.xml");
        yarnSiteConfigURL = cpRootFile.toURI().toURL();
    }

    private void configForTesting(String classPathRoot) throws URISyntaxException {
        yarnSiteConfigURL = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
        if (yarnSiteConfigURL == null) {
            throw new RuntimeException("Could not find 'yarn-site.xml' file in classpath");
        } else {
            LOG.info("Found 'yarn-site.xml' at "+ yarnSiteConfigURL.toURI().toString());
        }

        conf = new YarnConfiguration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");

        keytab = classPathRoot.substring(0, classPathRoot.lastIndexOf('/'))+"/splice.keytab";
        if (secure) {
            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("yarn.resourcemanager.principal", "yarn/example.com@EXAMPLE.COM");
            conf.set("yarn.resourcemanager.keytab", keytab);
            conf.set("yarn.nodemanager.principal", "yarn/example.com@EXAMPLE.COM");
            conf.set("yarn.nodemanager.keytab", keytab);
        }
        conf.setDouble("yarn.nodemanager.resource.io-spindles",2.0);
        conf.set("fs.default.name", "file:///");
        conf.set("yarn.nodemanager.container-executor.class","org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor");
        System.setProperty("zookeeper.sasl.client", "false");
        System.setProperty("zookeeper.sasl.serverconfig", "fake");

        conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_INTERVAL);
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
        conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
        conf.set("yarn.application.classpath", new File(yarnSiteConfigURL.getPath()).getParent());
    }

    private static void waitForNMToRegister(NodeManager nm)
        throws Exception {
        Utils.waitForNMToRegister(nm);
    }

    public ResourceManager getResourceManager() {
        return ((MiniYARNClusterSplice)yarnCluster).getResourceManager();
    }

    private NodeManager getNodeManager() {
        return ((MiniYARNClusterSplice)yarnCluster).getNodeManager(0);
    }
}
