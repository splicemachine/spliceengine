package com.splicemachine.derby.lifecycle;

import com.google.common.net.HostAndPort;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.olap.OlapServerNotReadyException;
import com.splicemachine.olap.OlapServerProvider;
import com.splicemachine.olap.OlapServerZNode;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.primitives.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class OlapServerProviderImpl implements OlapServerProvider {
    private static final Logger LOG = Logger.getLogger(OlapServerProviderImpl.class);

    private final SConfiguration config;
    private final Clock clock;
    private final HBaseConnectionFactory hbcf;

    public OlapServerProviderImpl(SConfiguration config, Clock clock, HBaseConnectionFactory hbcf) {
        this.config = config;
        this.clock = clock;
        this.hbcf = hbcf;
    }

    @Override
    public HostAndPort olapServerHost(String queue) throws IOException {
        try {
            if (config.getOlapServerExternal()) {
                byte[] bytes = null;
                int tries = 0;
                OlapServerNotReadyException osnr = null;
                String root = HConfiguration.getConfiguration().getSpliceRootPath() + HBaseConfiguration.OLAP_SERVER_PATH + HBaseConfiguration.OLAP_SERVER_QUEUE_PATH;
                while (tries < config.getOlapServerMaxRetries()) {
                    tries++;
                    try {
                        List<String> servers = ZkUtils.getChildren(root, false);
                        OlapServerZNode node = servers.stream()
                                .map(OlapServerZNode::parseFrom)
                                .filter(n -> n.getQueueName().equals(queue))
                                .sorted()
                                .findFirst().orElseThrow(() -> new OlapServerNotReadyException(queue, servers));
                        bytes = ZkUtils.getData(root + "/" + node.toZNode());
                        break;
                    } catch (IOException e) {
                        if (e instanceof OlapServerNotReadyException) {
                            osnr = (OlapServerNotReadyException) e;
                            // sleep & retry
                            try {
                                long pause = PipelineUtils.getPauseTime(tries, 10);
                                LOG.warn("Couldn't find OlapServer znode after " + tries + " retries, sleeping for " + pause + " ms", e);
                                clock.sleep(pause, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException ie) {
                                throw new IOException(ie);
                            }
                        } else {
                            throw e;
                        }
                    }
                }
                if (bytes == null) {
                    assert osnr != null;
                    String path = HConfiguration.getConfiguration().getSpliceRootPath() + HBaseConfiguration.OLAP_SERVER_PATH + HBaseConfiguration.OLAP_SERVER_DIAGNOSTICS_PATH + "/";
                    String sparkDiagnostics = getDiagnostics(path + "spark-" + queue);
                    String deploymentDiagnostics = getDiagnostics(path + queue);
                    String diagnostics = "";
                    if (sparkDiagnostics != null) {
                        diagnostics = "Spark diagnostics: " + sparkDiagnostics + "\n";
                    }
                    if (deploymentDiagnostics != null) {
                        diagnostics += "Deployment diagnostics:" + deploymentDiagnostics;
                    }
                    if (diagnostics.isEmpty()) {
                        diagnostics = "No diagnostics available, contact your system administrator";
                    }
                    osnr.setDiagnostics(diagnostics);
                    throw osnr;
                }
                String hostAndPort = Bytes.toString(bytes);
                return HostAndPort.fromString(hostAndPort);
            } else {
                return HostAndPort.fromParts(hbcf.getMasterServer().getHostname(), config.getOlapServerBindPort());
            }
        } catch (SQLException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException)
                throw (IOException) cause;
            else
                throw new IOException(e);
        }
    }

    private String getDiagnostics(String path) {
        try {
            byte[] bytes = ZkUtils.getData(path);
            return Bytes.toString(bytes);
        } catch (Exception e) {
            // ignore exception
        }
        return null;
    }
}