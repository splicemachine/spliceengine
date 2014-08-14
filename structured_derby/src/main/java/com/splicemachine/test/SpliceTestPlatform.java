package com.splicemachine.test;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.scheduler.SchedulerTracer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.log4j.Logger;

public class SpliceTestPlatform {

    private static final Logger LOG = Logger.getLogger(SpliceTestPlatform.class);

    private String hbaseRootDirUri;
    private Integer masterPort;
    private Integer masterInfoPort;
    private Integer regionServerPort;
    private Integer regionServerInfoPort;
    private Integer derbyPort = SpliceConstants.DEFAULT_DERBY_BIND_PORT;
    private boolean failTasksRandomly;


    public static void main(String[] args) throws Exception {

        SpliceTestPlatform spliceTestPlatform;
        if (args.length == 3) {
            spliceTestPlatform = new SpliceTestPlatform(args[0], args[1], args[2]);
            spliceTestPlatform.start();
        } else if (args.length == 7) {
            spliceTestPlatform = new SpliceTestPlatform(args[0], args[1], new Integer(args[2]), new Integer(args[3]), new Integer(args[4]), new Integer(args[5]), args[6]);
            spliceTestPlatform.start();

        } else if (args.length == 8) {
            spliceTestPlatform = new SpliceTestPlatform(args[0], args[1], new Integer(args[2]), new Integer(args[3]), new Integer(args[4]), new Integer(args[5]), new Integer(args[6]), args[7]);
            spliceTestPlatform.start();

        } else {
            System.out.println("Splice TestContext Platform supports arguments indicating the zookeeper target " +
                    "directory, hbase directory URI and a boolean flag indicating whether the chaos monkey should randomly fail tasks.");
            System.exit(1);
        }
    }

    public SpliceTestPlatform(String zookeeperTargetDirectory, String hbaseRootDirUri, String failTasksRandomly) {
        this(zookeeperTargetDirectory, hbaseRootDirUri, null, null, null, null, failTasksRandomly);
    }

    public SpliceTestPlatform(String zookeeperTargetDirectory,
                              String hbaseRootDirUri,
                              Integer masterPort,
                              Integer masterInfoPort,
                              Integer regionServerPort,
                              Integer regionServerInfoPort,
                              String failTasksRandomly) {

        this.hbaseRootDirUri = hbaseRootDirUri;
        this.masterPort = masterPort;
        this.masterInfoPort = masterInfoPort;
        this.regionServerPort = regionServerPort;
        this.regionServerInfoPort = regionServerInfoPort;
        this.failTasksRandomly = failTasksRandomly.equalsIgnoreCase("true");
    }

    public SpliceTestPlatform(String zookeeperTargetDirectory,
                              String hbaseRootDirUri,
                              Integer masterPort,
                              Integer masterInfoPort,
                              Integer regionServerPort,
                              Integer regionServerInfoPort,
                              Integer derbyPort,
                              String failTasksRandomly) {
        this(zookeeperTargetDirectory, hbaseRootDirUri, masterPort, masterInfoPort, regionServerPort, regionServerInfoPort, failTasksRandomly);
        this.derbyPort = derbyPort;
    }

    public void start() throws Exception {
        Configuration config = SpliceTestPlatformConfig.create(
                hbaseRootDirUri,
                masterPort,
                masterInfoPort,
                regionServerPort,
                regionServerInfoPort,
                derbyPort,
                failTasksRandomly
        );
        LOG.info("Derby listen port: " + derbyPort);
        LOG.info("Random task failure " + (this.failTasksRandomly ? "WILL" : "WILL NOT") + " occur");
        new MiniHBaseCluster(config, 1, 1);
    }

    public void end() throws Exception {
        SchedulerTracer.registerTaskStart(null);
        SchedulerTracer.registerTaskEnd(null);
    }

}