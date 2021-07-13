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

package com.splicemachine.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * Starts Yarn server
 */
public class SpliceTestDfsPlatform {
    public static int DEFAULT_NODE_COUNT = 1;

    private static final Logger LOG = Logger.getLogger(SpliceTestDfsPlatform.class);

    private MiniDFSCluster dfsCluster = null;
    private Configuration conf = null;

    public SpliceTestDfsPlatform() {
    }

    public static void main(String[] args) throws Exception {
        int nodeCount = DEFAULT_NODE_COUNT;
        if (args != null && args.length > 0) {
            nodeCount = Integer.parseInt(args[0]);
        } else {
            throw new RuntimeException("Use main method for testing with splice hdfs cluster.");
        }
        String directory = args[1];

        SpliceTestDfsPlatform hdfsParticipant = new SpliceTestDfsPlatform();
        hdfsParticipant.start(nodeCount, directory);
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
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            conf.writeXml(bytesOut);
            bytesOut.close();
            //write the bytes to the file in the classpath
            OutputStream os = new FileOutputStream(new File(new File(directory, "classes"), "core-site.xml"));
            os.write(bytesOut.toByteArray());
            os.close();
            
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
            fileSystem.setOwner(spliceUser, "splice", "splice");
        }
        LOG.info("HDFS cluster started, listening on port " + dfsCluster.getNameNodePort() + " writing to " + dfsCluster.getDataDirectory());
        LOG.info("Configuration " + dfsCluster.getConfiguration(0));
    }

}
