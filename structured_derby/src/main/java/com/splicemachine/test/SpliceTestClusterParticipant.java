package com.splicemachine.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;

/**
 * Add an additional member to the cluster started with SpliceTestPlatform.
 *
 * Although MiniHBaseCluster can be used to create a multi region-server cluster all within a single JVM our system
 * currently requires each member to be in a separate JVM (because of static-state/singletons, SpliceDriver, for instance).
 *
 * Running: mvn exec:exec -PspliceClusterMember
 */
public class SpliceTestClusterParticipant {

    private final int regionServerPort;
    private final int regionServerInfoPort;
    private final String hbaseTargetDirectory;
    private final int derbyPort;

    /**
     * MAIN:
     * <p/>
     * arg-1: hbase dir
     * arg-2: region server port
     * arg-3: region server info web-ui port
     * arg-4: derby port
     */
    public static void main(String[] args) throws Exception {
        SpliceTestClusterParticipant spliceTestPlatform;
        if (args.length == 4) {
            spliceTestPlatform = new SpliceTestClusterParticipant(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            spliceTestPlatform.start();
        } else {
            System.out.println("" +
                    "Splice Test Platform requires three arguments corresponding to the hbase target directory, " +
                    "region server port and region server info port.  The first region server is brought up via the " +
                    "test platform on 60010 and 60020.");
            System.exit(1);
        }
    }

    public SpliceTestClusterParticipant(String hbaseTargetDirectory, int regionServerPort, int regionServerInfoPort, int derbyPort) {
        this.hbaseTargetDirectory = hbaseTargetDirectory;
        this.regionServerPort = regionServerPort;
        this.regionServerInfoPort = regionServerInfoPort;
        this.derbyPort = derbyPort;
    }

    private void start() throws Exception {
        Configuration config = SpliceTestPlatformConfig.create(
                hbaseTargetDirectory,
                0,
                0,
                regionServerPort,
                regionServerInfoPort,
                derbyPort,
                false
        );

        MiniHBaseCluster miniHBaseCluster = new MiniHBaseCluster(config, 0, 1);
        miniHBaseCluster.startRegionServer();
    }

}
