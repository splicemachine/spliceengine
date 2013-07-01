package com.splicemachine.test;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.si.coprocessors.SIObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import com.splicemachine.derby.hbase.SpliceDerbyCoprocessor;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexManagementEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import com.splicemachine.derby.hbase.SpliceMasterObserver;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;

public class SpliceTestClusterParticipant extends TestConstants {
	protected MiniHBaseCluster miniHBaseCluster;
	protected int port;
	protected int infoPort;
	protected String hbaseTargetDirectory;
	
		
	public SpliceTestClusterParticipant(String hbaseTargetDirectory, int port, int infoPort) {
		this.hbaseTargetDirectory = hbaseTargetDirectory;
		this.port = port;
		this.infoPort = infoPort;
	}

	public static void main(String[] args) throws Exception {
		SpliceTestClusterParticipant spliceTestPlatform;
		if (args.length == 3) {
			spliceTestPlatform = new SpliceTestClusterParticipant(args[0],Integer.parseInt(args[1]),Integer.parseInt(args[2]));
			spliceTestPlatform.start();
		}
		if (args.length <= 2 || args.length > 3) {
			System.out.println("Splice Test Platform requires three arguments corresponding to the hbase target directory, region servier port and region server info port.  The first region server is brought up via the test platform on 60010 and 60020.");
			System.exit(1);
		}
	}
	
	public void start() throws Exception {		
		Configuration config = HBaseConfiguration.create();
		setBaselineConfigurationParameters(config);
		miniHBaseCluster = new MiniHBaseCluster(config,0,1);
		miniHBaseCluster.startRegionServer();
	}
	public void end() throws Exception {

	}

	public void setBaselineConfigurationParameters(Configuration configuration) {
		configuration.set("hbase.rootdir", "file://" + hbaseTargetDirectory);
		configuration.set("hbase.rpc.timeout", "6000");
		configuration.set("hbase.cluster.distributed", "true");
		configuration.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
		configuration.set("hbase.regionserver.handler.count", "40");
		configuration.setInt("hbase.regionserver.port", port);
		configuration.setInt("hbase.regionserver.info.port", infoPort);
		configuration.setInt(SpliceConstants.CONFIG_DERBY_BIND_PORT, 1528);
		coprocessorBaseline(configuration);
		configuration.reloadConfiguration();
		SpliceConstants.reloadConfiguration(configuration);
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
        configuration.set("hbase.coprocessor.master.classes", SpliceMasterObserver.class.getCanonicalName() + "");
    }

}
