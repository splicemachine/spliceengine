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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.log4j.Logger;

import com.splicemachine.access.configuration.SQLConfiguration;

/**
 * Add an additional member to the cluster started with SpliceTestPlatform.
 * <p/>
 * Although MiniHBaseCluster can be used to create a multi region-server cluster all within a single JVM our system
 * currently requires each member to be in a separate JVM (because of static-state/singletons, SpliceDriver, for instance).
 * <p/>
 * Running: mvn exec:exec -PspliceClusterMember
 */
public class SpliceTestClusterParticipant {

    private static final Logger LOG = Logger.getLogger(SpliceTestClusterParticipant.class);

    private static final int REGION_SERVER_PORT = 60020;
    private static final int REGION_SERVER_WEB_PORT = 60030;

    private final String hbaseTargetDirectory;
    private final int memberNumber;

    /**
     * MAIN:
     * <p/>
     * arg-1: hbase dir
     * arg-2: cluster member number
     */
    public static void main(String[] args) throws Exception {
        SpliceTestClusterParticipant spliceTestPlatform;
        if (args.length == 2) {
            spliceTestPlatform = new SpliceTestClusterParticipant(args[0], Integer.parseInt(args[1]));
            spliceTestPlatform.start();
        } else {
            System.out.println("usage: SpliceTestClusterParticipant [hbase dir] [member number]");
            System.exit(1);
        }
    }

    public SpliceTestClusterParticipant(String hbaseTargetDirectory, int memberNumber) {
        this.hbaseTargetDirectory = hbaseTargetDirectory;
        this.memberNumber = memberNumber;
    }

    private void start() throws Exception {
        int regionServerPort = REGION_SERVER_PORT + memberNumber;
        int regionServerInfoPort = REGION_SERVER_WEB_PORT + memberNumber;
        int derbyPort = SQLConfiguration.DEFAULT_NETWORK_BIND_PORT + memberNumber;

        Configuration config = SpliceTestPlatformConfig.create(
                hbaseTargetDirectory,
                0,
                0,
                regionServerPort,
                regionServerInfoPort,
                derbyPort,
                false,
                null
        );

        MiniHBaseCluster miniHBaseCluster = new MiniHBaseCluster(config, 0, 1);
        miniHBaseCluster.startRegionServer();
    }

}
