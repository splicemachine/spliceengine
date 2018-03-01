/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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

package com.splicemachine.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNClusterSplice;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Starts Yarn server
 */
public class SpliceTestDfsPlatform {
    public static int DEFAULT_NODE_COUNT = 1;

    private static final Logger LOG = Logger.getLogger(SpliceTestDfsPlatform.class);

    private MiniDFSCluster dfsCluster = null;
    private Configuration conf = null;

    public SpliceTestDfsPlatform() {
        // for testing
        try {
            configForTesting();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Error trying to config.", e);
        }
    }

    public static void main(String[] args) throws Exception {
        String classPathRoot;
        int nodeCount = DEFAULT_NODE_COUNT;
        if (args != null && args.length > 0) {
            nodeCount = Integer.parseInt(args[0]);
        } else {
            throw new RuntimeException("Use main method for testing with splice yarn client. First arg is required " +
                                           "is the path to the root of the server classpath. This is required so that " +
                                           "splice clients can find the server configuration (yarn-site.xml) in order " +
                                           "to connect.");
        }
        String directory = args[1];

        SpliceTestDfsPlatform yarnParticipant = new SpliceTestDfsPlatform();
//        yarnParticipant.configForSplice(classPathRoot);
        yarnParticipant.start(nodeCount, directory);
    }

    public Configuration getConfig() {
        return conf;
    }

    public MiniDFSCluster getDfsCluster() {
        return dfsCluster;
    }

    public void stop() {
        if (dfsCluster != null) {
            dfsCluster.shutdown();
        }
    }

    public void start(int nodeCount, String directory) throws Exception {
        if (dfsCluster == null) {
            conf = new Configuration();
            String keytab = directory+"/splice.keytab";
            conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, directory);
            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("dfs.namenode.kerberos.principal", "hdfs/example.com@EXAMPLE.COM");
            conf.set("dfs.namenode.keytab.file", keytab);
            conf.set("dfs.web.authentication.kerberos.principal", "hdfs/example.com@EXAMPLE.COM");
            conf.set("dfs.web.authentication.kerberos.keytab", keytab);
            conf.set("dfs.datanode.kerberos.principal", "hdfs/example.com@EXAMPLE.COM");
            conf.set("dfs.datanode.keytab.file", keytab);
            conf.set("dfs.block.access.token.enable", "true");
            conf.set(DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY, "true");

            dfsCluster = new MiniDFSCluster.Builder(conf).clusterId("localDfs").format(true).numDataNodes(nodeCount).nameNodePort(58878).build();
            dfsCluster.waitActive();

            conf = dfsCluster.getConfiguration(0);
            FileSystem fileSystem = FileSystem.get(conf);
            Path hbase = new Path("/hbase");
            fileSystem.mkdirs(hbase);
            fileSystem.setOwner(hbase, "hbase", "hbase");
            Path users = new Path("/user");
            fileSystem.mkdirs(users, FsPermission.createImmutable((short)0777));
            Path hbaseUser = new Path("/user/hbase");
            fileSystem.mkdirs(hbaseUser);
            fileSystem.setOwner(hbaseUser, "hbase", "hbase");
            Path spliceUser = new Path("/user/splice");
            fileSystem.mkdirs(spliceUser);
            fileSystem.setOwner(spliceUser, "hbase", "hbase");
        }
        LOG.info("HDFS cluster started, listening on port " + dfsCluster.getNameNodePort() + " writing to " + dfsCluster.getDataDirectory());
        LOG.info("Configuration " + dfsCluster.getConfiguration(0));
    }

    private void configForSplice(String classPathRoot) throws URISyntaxException, MalformedURLException {
//        LOG.info("Classpath root: "+classPathRoot);
//        if (classPathRoot == null || classPathRoot.isEmpty()) {
//            throw new RuntimeException("Can't find path to classpath root: "+classPathRoot);
//        }
//        File cpRootFile = new File(classPathRoot);
//        if (! cpRootFile.exists()) {
//            throw new RuntimeException("Can't find path to classpath root: "+classPathRoot);
//        }
//        cpRootFile = new File(classPathRoot, "/yarn-site.xml");
//        yarnSiteConfigURL = cpRootFile.toURI().toURL();
    }

    private void configForTesting() throws URISyntaxException {
//        yarnSiteConfigURL = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
//        if (yarnSiteConfigURL == null) {
//            throw new RuntimeException("Could not find 'yarn-site.xml' file in classpath");
//        } else {
//            LOG.info("Found 'yarn-site.xml' at "+ yarnSiteConfigURL.toURI().toString());
//        }
//
//        conf = new YarnConfiguration();
//        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
//        conf.setDouble("yarn.nodemanager.resource.io-spindles",2.0);
//        conf.set("fs.default.name", "file:///");
//        conf.set("yarn.nodemanager.container-executor.class","org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor");
//        System.setProperty("zookeeper.sasl.client", "false");
//        System.setProperty("zookeeper.sasl.serverconfig", "fake");
//
//        conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_INTERVAL);
//        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
//        conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
//        conf.set("yarn.application.classpath", new File(yarnSiteConfigURL.getPath()).getParent());
    }

}
