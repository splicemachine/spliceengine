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

            Configuration config = SpliceTestPlatformConfig.createForITs(
                    hbaseRootDirUri,
                    masterPort,
                    masterInfoPort,
                    regionServerPort,
                    regionServerInfoPort,
                    derbyPort,
                    failTasksRandomly);

            new MiniHBaseCluster(config, 1, 1);

        } catch (NumberFormatException e) {
            SpliceTestPlatformUsage.usage("Bad port specified", e);
        }
    }

}