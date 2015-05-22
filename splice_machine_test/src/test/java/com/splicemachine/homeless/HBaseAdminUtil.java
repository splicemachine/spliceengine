package com.splicemachine.homeless;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import com.splicemachine.derby.utils.SpliceUtils;

/**
 * Utility to work with HBaseAdmin in concert with testing - called from
 * tests, shell scripts, etc.
 * <p/>
 * Things you can do are: report status of a cluster, shut down a region server, ...
 * TODO: add more shit
 */
public class HBaseAdminUtil {
    private static final Logger LOG = Logger.getLogger(HBaseAdminUtil.class);

    public static HBaseAdmin getAdmin() {
        // TODO: requires a cast until we address deprecated use of HBaseAdmin
        return SpliceUtils.getAdmin();
    }

    /**
     * Report status/health of a cluster.
     *
     * @param admin optional HBase admin to use for performance considerations
     *              (it's an expensive request). If <code>null</code>, calls
     *              {@link #getAdmin()}.
     * @param ps PrintStream to which to write
     * @throws IOException when admin can't get cluster status
     */
    public static void clusterReport(HBaseAdmin admin, PrintStream ps) throws IOException {
        if (admin == null) {
            admin = getAdmin();
        }
        StringBuilder sb = new StringBuilder(1024);

        ClusterStatus status = admin.getClusterStatus();

        ServerName masterStatus = status.getMaster();
        sb.append("\nMaster:");
        sb.append("\n  ").append(masterStatus.getServerName());
        sb.append("\n  Coprocessors:");
        String[] mcps = status.getMasterCoprocessors();
        if (mcps.length == 0) {
            sb.append("\n    None");
        }
        for (String cp : mcps) {
            sb.append("\n    ").append(cp);
        }
        int backupMasters = status.getBackupMastersSize();
        sb.append("\n  Backup Masters: ").append(backupMasters);
        if (backupMasters > 0) {
            for (ServerName sn : status.getBackupMasters()) {
                sb.append("\n    ").append(sn.getServerName());
            }
        }
        sb.append("\nRegion Servers: ").append(status.getServersSize());
        for (ServerName sn : status.getServers()) {
            sb.append("\n  ").append(sn.getServerName());

            ServerLoad load = status.getLoad(sn);
            sb.append("\n    Info Port                 : ").append(load.getInfoServerPort());
            sb.append("\n    Coprocessors:");
            String[] rscs = load.getRsCoprocessors();
            if (rscs.length == 0) {
                sb.append("\n      None");
            }
            for (String cp : rscs) {
                sb.append("\n      ").append(cp);
            }
            sb.append("\n    Regions                   : ").append(load.getNumberOfRegions());
            sb.append("\n    Read Requests             : ").append(load.getReadRequestsCount());
            sb.append("\n    Write Requests            : ").append(load.getWriteRequestsCount());
            sb.append("\n    Requests Since Last Rpt   : ").append(load.getNumberOfRequests());

        }
        int deadServerSize = status.getDeadServers();
        sb.append("\nNumber of dead region servers: ").append(deadServerSize);
        if (deadServerSize > 0) {
            for (ServerName serverName: status.getDeadServerNames()) {
                sb.append("\n  ").append(serverName);
            }
        }
        sb.append("\nAverage load: ").append(status.getAverageLoad());
        sb.append("\nNumber of requests: ").append(status.getRequestsCount());
        sb.append("\nNumber of regions: ").append(status.getRegionsCount());

        ps.println(sb.toString());
    }

    /**
     * Request stopping the given regionServer.
     *
     * @param admin optional HBase admin to use for performance considerations
     *              (it's an expensive request). If <code>null</code>, calls
     *              {@link #getAdmin()}.
     * @param regionServer region server for which to request stop.
     * @throws IOException
     */
    public static void stopRegionServer(HBaseAdmin admin, ServerName regionServer) throws IOException{
        LOG.info("Requesting shutdown for: " + regionServer.getHostAndPort());
        if (admin == null) {
            admin = getAdmin();
        }
        admin.stopRegionServer(regionServer.getHostAndPort());
    }

    /**
     * Request stopping the first region server returned from ClusterStatus#getServers().
     *
     * @param admin optional HBase admin to use for performance considerations
     *              (it's an expensive request). If <code>null</code>, calls
     *              {@link #getAdmin()}.
     * @throws IOException
     */
    public static void stopFirstRegionServer(HBaseAdmin admin) throws IOException {
        if (admin == null) {
            admin = getAdmin();
        }
        ServerName sn = admin.getClusterStatus().getServers().iterator().next();
        stopRegionServer(admin, sn);
    }
}
