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

package com.splicemachine.test;

import com.splicemachine.hbase.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Start MiniHBaseCluster for use by ITs.
 */
public class SpliceTestPlatform {

    public static void main(String[] args) throws Exception {
        if (args.length != 7) {
            SpliceTestPlatformUsage.usage("Unknown argument(s)", null);
        }
        try {

            String hbaseRootDirUri = args[0];
            Integer masterPort = Integer.valueOf(args[1]);
            Integer masterInfoPort = Integer.valueOf(args[2]);
            Integer regionServerPort = Integer.valueOf(args[3]);
            Integer regionServerInfoPort = Integer.valueOf(args[4]);
            Integer derbyPort = Integer.valueOf(args[5]);
            boolean failTasksRandomly = Boolean.valueOf(args[6]);

            Configuration config = SpliceTestPlatformConfig.create(
                    hbaseRootDirUri,
                    masterPort,
                    masterInfoPort,
                    regionServerPort,
                    regionServerInfoPort,
                    derbyPort,
                    failTasksRandomly);

            // clean-up zookeeper
            try {
                ZkUtils.delete("/hbase/master");
            } catch (KeeperException.NoNodeException ex) {
                // ignore
            }
            try {
                ZkUtils.recursiveDelete("/hbase/rs");
            } catch (KeeperException.NoNodeException | IOException ex) {
                // ignore
            }

            MiniHBaseCluster miniHBaseCluster = new MiniHBaseCluster(config, 1, 1);

            new SpliceTestPlatformShutdownThread(miniHBaseCluster);

        } catch (NumberFormatException e) {
            SpliceTestPlatformUsage.usage("Bad port specified", e);
        }
    }

}