package com.splicemachine.single;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDerbyCoprocessor;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexManagementEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import com.splicemachine.derby.hbase.SpliceMasterObserver;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.derby.impl.job.scheduler.SchedulerTracer;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.coprocessors.TimestampMasterObserver;

import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ServerAdminClient;

public class SpliceSinglePlatform extends ServerAdminClient {
    private static final Logger LOG = Logger.getLogger(SpliceSinglePlatform.class);
	protected MiniHBaseCluster miniHBaseCluster;
	protected String zookeeperTargetDirectory;
	protected String hbaseRootDirUri;
    protected Integer masterPort;
    protected Integer masterInfoPort;
    protected Integer regionServerPort;
    protected Integer regionServerInfoPort;
    protected Integer derbyPort = SpliceConstants.DEFAULT_DERBY_BIND_PORT;

    public SpliceSinglePlatform(String zookeeperTargetDirectory, String hbaseRootDirUri, Integer masterPort,
                              Integer masterInfoPort, Integer regionServerPort, Integer regionServerInfoPort, Integer derbyPort, String chaos) {
        this.zookeeperTargetDirectory = zookeeperTargetDirectory;
        this.hbaseRootDirUri = hbaseRootDirUri;

        this.masterPort = masterPort;
        this.masterInfoPort = masterInfoPort;
        this.regionServerPort = regionServerPort;
        this.regionServerInfoPort = regionServerInfoPort;
        this.derbyPort = derbyPort;
    }

	public static void main(String[] args) {
		SpliceSinglePlatform spliceSinglePlatform;
		try {
			if (args.length == 8) {
			    spliceSinglePlatform = new SpliceSinglePlatform(args[0], args[1], new Integer(args[2]), new Integer(args[3]), new Integer(args[4]), new Integer(args[5]), new Integer(args[6]), args[7]);
			    spliceSinglePlatform.start();

			}else{
				usage("Unknown argument(s)", null);
				System.exit(1);
			}
		} catch (NumberFormatException e) {
			usage("Bad port specified",e);
			System.exit(1);
		} catch (Exception e) {
			usage("Unknown exception",e);
			System.exit(1);
		}
	}
	
	private static void usage(String msg, Throwable t) {
		PrintStream out = System.out;
		if (t != null) {
			out = System.err;
		}
		if (msg != null) {
			out.println(msg);
		}
		if (t != null) {
			t.printStackTrace(out);
		}
		out.println("Usage: SpliceSinglePlatform( String zookeeperTargetDirectory, String hbaseRootDirUri, Integer masterPort, " +
                              "Integer masterInfoPort, Integer regionServerPort, Integer regionServerInfoPort, Integer derbyPort, String true|false )");
	}
	
	public void start() throws Exception {
		Configuration config = HBaseConfiguration.create();
		setBaselineConfigurationParameters(config);
        LOG.info("Derby listen port: "+derbyPort);
		miniHBaseCluster = new MiniHBaseCluster(config,1,1);
	}
	
	public static void stop(String host, int port) {
		kill(host, port);
	}

	public void end() throws Exception {
        SchedulerTracer.registerTaskStart(null);
        SchedulerTracer.registerTaskEnd(null);
	}

    private void setInt(Configuration configuration, String property, Integer intProperty){
        if(intProperty != null){
            configuration.setInt(property, intProperty);
        }
    }

	public void setBaselineConfigurationParameters(Configuration configuration) {
        if (hbaseRootDirUri != null && ! hbaseRootDirUri.equals("CYGWIN")) {
            // Must allow Cygwin instance to config its own rootURI
		    configuration.set("hbase.rootdir", hbaseRootDirUri);
        }
		configuration.setInt("hbase.rpc.timeout", 120000);
		configuration.setInt("hbase.regionserver.lease.period", 120000);		
		configuration.set("hbase.cluster.distributed", "true");
		configuration.set("hbase.master.distributed.log.splitting", "false");
		configuration.setInt("hbase.balancer.period", 10000);
		configuration.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
		configuration.set("hbase.regionserver.handler.count", "60");
        setInt(configuration, "hbase.master.port", masterPort);
        setInt(configuration, "hbase.master.info.port", masterInfoPort);
        setInt(configuration, "hbase.regionserver.port", regionServerPort);
        setInt(configuration, "hbase.regionserver.info.port", regionServerInfoPort);
        configuration.setInt(SpliceConstants.DERBY_BIND_PORT, derbyPort);
        configuration.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, true);
        configuration.setInt("hbase.hstore.blockingStoreFiles", 20);
        configuration.setInt("hbase.hregion.memstore.block.multiplier", 4);
        configuration.setFloat("hbase.store.compaction.ratio", (float) 0.25);
        configuration.setFloat("io.hfile.bloom.error.rate", (float)0.005);
        configuration.setInt("hbase.master.event.waiting.time", 20);
        configuration.setInt("hbase.client.pause", 1000);
        configuration.setInt("hbase.master.lease.thread.wakefrequency", 3000);
        configuration.setInt("hbase.server.thread.wakefrequency", 1000);
        configuration.setInt("hbase.regionserver.msginterval", 1000);
        configuration.set("hbase.regionserver.region.split.policy", "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy");
        String interfaceName = "lo0";
        if (!System.getProperty("os.name").contains("Mac")) {
        	interfaceName = "default";
        }
        configuration.set("hbase.zookeeper.dns.interface", interfaceName);
        configuration.set("hbase.regionserver.dns.interface", interfaceName);
        configuration.set("hbase.master.dns.interface", interfaceName);
        configuration.setLong(HConstants.HREGION_MAX_FILESIZE, 128 * 1024 * 1024L);

        coprocessorBaseline(configuration);
		configuration.reloadConfiguration();
		SIConstants.reloadConfiguration(configuration);
	}

    public void coprocessorBaseline(Configuration configuration) {
        configuration.set("hbase.coprocessor.region.classes",
                SpliceOperationRegionObserver.class.getCanonicalName() + "," +
                        SpliceIndexObserver.class.getCanonicalName() + "," +
                        SpliceDerbyCoprocessor.class.getCanonicalName() + "," +
                        SpliceIndexManagementEndpoint.class.getCanonicalName() + "," +
                        SpliceIndexEndpoint.class.getCanonicalName() + "," +
                        CoprocessorTaskScheduler.class.getCanonicalName()+","+
                        SIObserver.class.getCanonicalName()
        );
        configuration.set("hbase.coprocessor.master.classes",
			SpliceMasterObserver.class.getCanonicalName() + "," +
			TimestampMasterObserver.class.getCanonicalName());
    }

}
