package com.splicemachine.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;

/**
 * -Dlog4j.configuration=file:src/main/resources/log4j.properties
 */
public class SpliceTestClusterParticipant {

    private final int regionServerPort;
    private final int regionServerInfoPort;
    private final String hbaseTargetDirectory;

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
    }

    private void start() throws Exception {
        Configuration config = SpliceTestPlatformConfig.create(
                hbaseTargetDirectory,
                0,
                0,
                regionServerPort + 1,
                regionServerInfoPort + 1,
                1528,
                false
        );

        MiniHBaseCluster miniHBaseCluster = new MiniHBaseCluster(config, 0, 1);
        miniHBaseCluster.startRegionServer();
    }

}
