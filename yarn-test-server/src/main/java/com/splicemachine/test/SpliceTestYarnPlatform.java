package com.splicemachine.test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.log4j.Logger;

/**
 * Starts Yarn server
 */
public class SpliceTestYarnPlatform {
    public static final int DEFAULT_HEARTBEAT_INTERVAL = 100;

    private static final Logger LOG = Logger.getLogger(SpliceTestYarnPlatform.class);
    private static int DEFAULT_NODE_COUNT = 3;

    private MiniYARNCluster yarnCluster = null;
    private Configuration conf = null;

    public SpliceTestYarnPlatform() {
        // for testing
    }

    public static void main(String[] args) throws Exception {
        int nodeCount = DEFAULT_NODE_COUNT;
        if (args != null && args.length > 0) {
            nodeCount = Integer.parseInt(args[0]);
        }

        SpliceTestYarnPlatform yarnParticipant = new SpliceTestYarnPlatform();
        yarnParticipant.start(nodeCount);
    }

    public Configuration getConfig() {
        return conf;
    }

    public MiniYARNCluster getYarnCluster() {
        return yarnCluster;
    }

    public void stop() {
        if (yarnCluster != null && yarnCluster.getServiceState() == Service.STATE.STARTED) {
            yarnCluster.stop();
        }
    }

    public void start(int nodeCount) throws Exception {
        if (yarnCluster == null) {
            LOG.info("Starting up YARN cluster with "+nodeCount+" nodes.");

            URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
            if (url == null) {
                throw new RuntimeException("Could not find 'yarn-site.xml' file in classpath");
            } else {
                LOG.info("Found 'yarn-site.xml' at "+url.toURI().toString());
            }

            conf = new YarnConfiguration();
            // TODO: JC - take args from mvn exec
            conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_INTERVAL);
            conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
            conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
            conf.set("yarn.application.classpath", new File(url.getPath()).getParent());

            yarnCluster = new MiniYARNCluster(SpliceTestYarnPlatform.class.getSimpleName(), nodeCount, 1, 1);
            yarnCluster.init(conf);
            yarnCluster.start();

            NodeManager nm = yarnCluster.getNodeManager(0);
            waitForNMToRegister(nm);
        }
        LOG.info("YARN cluster started.");
    }

    private static void waitForNMToRegister(NodeManager nm)
        throws Exception {
        int attempt = 60;
        ContainerManagerImpl cm =
            ((ContainerManagerImpl) nm.getNMContext().getContainerManager());
        while (cm.getBlockNewContainerRequestsStatus() && attempt-- > 0) {
            Thread.sleep(2000);
        }
    }
}
