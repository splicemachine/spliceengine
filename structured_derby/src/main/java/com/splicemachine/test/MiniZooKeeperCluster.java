package com.splicemachine.test;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MiniZooKeeperCluster {

    private static final Logger LOG = Logger.getLogger(MiniZooKeeperCluster.class);
    protected boolean started;
    protected ExecutorService service;

    public MiniZooKeeperCluster() {
        this.started = false;
    }

    public int startup(File baseDir) throws IOException, InterruptedException {
        return startup(baseDir, 1);
    }

    public int startup(File baseDir, int numZooKeeperServers) throws IOException, InterruptedException {
        if (numZooKeeperServers <= 0)
            return -1;
        shutdown();
        service = Executors.newFixedThreadPool(numZooKeeperServers);
        // running all the ZK servers
        for (int i = 0; i < numZooKeeperServers; i++) {
            int l = i + 1;
            File dir = new File(baseDir, "zookeeper_" + l).getAbsoluteFile();
            prepareDir(dir, i + 1);
            Properties startupProperties = new Properties();
            startupProperties.setProperty("tickTime", "2000");
            startupProperties.setProperty("dataDir", dir.getAbsolutePath());
            startupProperties.setProperty("initLimit", "10");
            startupProperties.setProperty("syncLimit", "5");
            startupProperties.setProperty("maxClientCnxns", "100");

            for (int j = 0; j < numZooKeeperServers; j++) {
                int m = j + 1;
                startupProperties.setProperty("server." + m, "localhost:" + (2888 + j) + ":" + (3888 + j));
            }
            startupProperties.setProperty("clientPort", String.valueOf(2181 + i));
            LOG.trace("startup Properties: " + startupProperties);
            QuorumPeerConfig config = new QuorumPeerConfig();
            try {
                config.parseProperties(startupProperties);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            service.execute(new SpliceZoo(config, l));
        }
        started = true;
        return 0;
    }

    private void prepareDir(File dir, int i) throws IOException {
        try {
            if (!dir.exists()) {
                dir.mkdirs();
                FileUtils.writeStringToFile(new File(dir, "myid"), "" + i);
            }
        } catch (SecurityException e) {
            throw new IOException("creating dir: " + dir, e);
        }
    }

    public void shutdown() throws IOException {
        if (!started) {
            return;
        }
        service.shutdown();
        started = false;
        LOG.info("Shutdown MiniZK cluster with all ZK servers");
    }

}
