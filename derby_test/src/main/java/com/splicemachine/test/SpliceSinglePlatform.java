package com.splicemachine.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;

import static com.splicemachine.test.SpliceTestPlatformUsage.usage;

/**
 * Start MiniHBaseCluster for use by demo/standalone splice app.
 */
public class SpliceSinglePlatform {

    public static void main(String[] args) throws Exception {
        if (args.length != 7) {
            usage("Unknown argument(s)", null);
        }
        try {

            String hbaseRootDirUri = args[0];
            Integer masterPort = Integer.valueOf(args[1]);
            Integer masterInfoPort = Integer.valueOf(args[2]);
            Integer regionServerPort = Integer.valueOf(args[3]);
            Integer regionServerInfoPort = Integer.valueOf(args[4]);
            Integer derbyPort = Integer.valueOf(args[5]);
            boolean failTasksRandomly = Boolean.valueOf(args[6]);

            Configuration config = SpliceTestPlatformConfig.createForDemoApp(
                    hbaseRootDirUri,
                    masterPort,
                    masterInfoPort,
                    regionServerPort,
                    regionServerInfoPort,
                    derbyPort,
                    failTasksRandomly);

            new MiniHBaseCluster(config, 1, 1);

        } catch (NumberFormatException e) {
            usage("Bad port specified", e);
        }
    }

}