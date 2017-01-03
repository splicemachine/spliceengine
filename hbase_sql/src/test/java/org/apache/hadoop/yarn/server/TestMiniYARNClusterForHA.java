/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.yarn.server;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

@Ignore
public class TestMiniYARNClusterForHA {
  MiniYARNCluster cluster;
    private static final File testDir = new File("target",
            TestDiskFailures.class.getName()).getAbsoluteFile();
    private static final File localFSDirBase = new File(testDir,
            TestDiskFailures.class.getName() + "-localDir");
    private static FileContext localFS = null;

    @Before
  public void setup() throws IOException, InterruptedException {
        localFS = FileContext.getLocalFSFileContext();
        localFS.delete(new Path(localFSDirBase.getAbsolutePath()), true);
        localFSDirBase.mkdirs();
        String localDir1 = new File(testDir, "localDir1").getPath();
        String localDir2 = new File(testDir, "localDir2").getPath();
        String logDir1 = new File(testDir, "logDir1").getPath();
        String logDir2 = new File(testDir, "logDir2").getPath();

        Configuration conf = new YarnConfiguration();
        conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir1 + "," + localDir2);
        conf.set(YarnConfiguration.NM_LOG_DIRS, logDir1 + "," + logDir2);
        conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:0");
    conf.set("yarn.resourcemanager.hostname.rm0","localhost");
    conf.set("yarn.resourcemanager.hostname.rm1","localhost");
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "splice:///");
    conf.set("fs.default.name", "splice:///");
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    conf.set("fs.hdfs.client", "org.apache.hadoop.hdfs.DistributedFileSystem");
    conf.set("yarn.nodemanager.aux-services","");
    cluster = new MiniYARNCluster(TestMiniYARNClusterForHA.class.getName(),
        2, 1, 1, 1);
    cluster.init(conf);
    cluster.start();

    cluster.getResourceManager(0).getRMContext().getRMAdminService()
        .transitionToActive(new HAServiceProtocol.StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER));

      Assert.assertFalse("RM never turned active", -1 == cluster.getActiveRMIndex());
  }

  @Test
  public void testClusterWorks() throws YarnException, InterruptedException {
    Assert.assertTrue("NMs fail to connect to the RM",
            cluster.waitForNodeManagersToConnect(5000));
  }
}