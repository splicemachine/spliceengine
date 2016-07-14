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

package com.splicemachine.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;

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

            MiniHBaseCluster miniHBaseCluster = new MiniHBaseCluster(config, 1, 1);

            new SpliceTestPlatformShutdownThread(miniHBaseCluster);

        } catch (NumberFormatException e) {
            SpliceTestPlatformUsage.usage("Bad port specified", e);
        }
    }

}