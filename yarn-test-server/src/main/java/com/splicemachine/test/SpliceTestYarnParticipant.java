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
public class SpliceTestYarnParticipant {
    private static final Logger LOG = Logger.getLogger(SpliceTestYarnParticipant.class);
    private static int DEFAULT_NODE_COUNT = 3;

    private MiniYARNCluster yarnCluster = null;
    private Configuration conf = null;

    public SpliceTestYarnParticipant() {
        // default visibility
    }

    public static void main(String[] args) throws Exception {
        int nodeCount = DEFAULT_NODE_COUNT;
        if (args != null && args.length > 0) {
            nodeCount = Integer.parseInt(args[0]);
        }

        SpliceTestYarnParticipant yarnParticipant = new SpliceTestYarnParticipant();
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
            LOG.info("Starting up YARN cluster");

            URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
            if (url == null) {
                throw new RuntimeException("Could not find 'yarn-site.xml' file in classpath");
            } else {
                LOG.info("Found 'yarn-site.xml' at "+url.toURI().toString());
            }

            conf = new YarnConfiguration();
            conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
            conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
            yarnCluster = new MiniYARNCluster(SpliceTestYarnParticipant.class.getSimpleName(), nodeCount, 1, 1);
            yarnCluster.init(conf);
            yarnCluster.start();

            NodeManager nm = yarnCluster.getNodeManager(0);
            waitForNMToRegister(nm);

            Configuration yarnClusterConfig = yarnCluster.getConfig();
            yarnClusterConfig.set("yarn.application.classpath", new File(url.getPath()).getParent());
            //write the document to a buffer (not directly to the file, as that
            //can cause the file being written to get read -which will then fail.
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            yarnClusterConfig.writeXml(bytesOut);
            bytesOut.close();
            //write the bytes to the file in the classpath
            OutputStream os = new FileOutputStream(new File(url.getPath()));
            os.write(bytesOut.toByteArray());
            os.close();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
            }
        }
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
