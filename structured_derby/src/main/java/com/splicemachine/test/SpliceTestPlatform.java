package com.splicemachine.test;

import java.io.File;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.si.coprocessors.SIObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.log4j.Logger;

import com.splicemachine.derby.hbase.SpliceDerbyCoprocessor;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexManagementEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;

public class SpliceTestPlatform extends TestConstants {
	private static final Logger LOG = Logger.getLogger(SpliceTestPlatform.class);
	protected MiniZooKeeperCluster miniZooKeeperCluster;
	protected MiniHBaseCluster miniHBaseCluster;
	protected MiniHBaseCluster miniHBaseCluster2;
	protected String zookeeperTargetDirectory;
	protected String hbaseTargetDirectory;
	
	public SpliceTestPlatform() {
		super();
	}
	
	public SpliceTestPlatform(String targetDirectory) {
		this(targetDirectory + "zookeeper",targetDirectory + "hbase");
	}

	public SpliceTestPlatform(String zookeeperTargetDirectory, String hbaseTargetDirectory) {
		this.zookeeperTargetDirectory = zookeeperTargetDirectory;
		this.hbaseTargetDirectory = hbaseTargetDirectory;
	}

	public static void main(String[] args) throws Exception {
		SpliceTestPlatform spliceTestPlatform;
		if (args.length == 1) {
			spliceTestPlatform = new SpliceTestPlatform(args[0]);
		}
		if (args.length == 2) {
			spliceTestPlatform = new SpliceTestPlatform(args[0],args[1]);
			spliceTestPlatform.start();
		}
		if (args.length == 0 || args.length > 2) {
			System.out.println("Splice Test Platform supports one argument providing the target directory" +
					" or two arguments dictating the zookeeper and hbase directory.");
			System.exit(1);
		}
	}
	
	public void start() throws Exception {
		Configuration config = HBaseConfiguration.create();
		setBaselineConfigurationParameters(config);
		miniZooKeeperCluster = new MiniZooKeeperCluster();
		miniZooKeeperCluster.startup(new File(zookeeperTargetDirectory),3);
		miniHBaseCluster = new MiniHBaseCluster(config,1,1);
		Configuration config2 = new Configuration(config);
		config2.setInt("hbase.regionserver.port", 60021);
		config2.setInt("hbase.regionserver.info.port", 60031);
		miniHBaseCluster2 = new MiniHBaseCluster(config2,0,1); // Startup Second Server on different ports		
	}
	public void end() throws Exception {

	}

	public void setBaselineConfigurationParameters(Configuration configuration) {
		configuration.set("hbase.rootdir", "file://" + hbaseTargetDirectory);
		configuration.set("hbase.rpc.timeout", "6000");
		configuration.set("hbase.cluster.distributed", "true");
		configuration.set("hbase.zookeeper.quorum", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
		configuration.set("hbase.regionserver.handler.count", "40");
		coprocessorBaseline(configuration);
		configuration.reloadConfiguration();
	}

    public void coprocessorBaseline(Configuration configuration) {
        configuration.set("hbase.coprocessor.region.classes",
                SpliceOperationRegionObserver.class.getCanonicalName() + "," +
                        SpliceOperationCoprocessor.class.getCanonicalName() + "," +
                        SpliceIndexObserver.class.getCanonicalName() + "," +
                        SpliceDerbyCoprocessor.class.getCanonicalName() + "," +
                        SpliceIndexManagementEndpoint.class.getCanonicalName() + "," +
                        SpliceIndexEndpoint.class.getCanonicalName() + "," +
                        CoprocessorTaskScheduler.class.getCanonicalName()+","+
                        SIObserver.class.getCanonicalName()
        );


    }

}
