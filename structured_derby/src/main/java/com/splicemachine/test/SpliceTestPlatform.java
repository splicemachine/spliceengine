package com.splicemachine.test;

import com.google.common.base.Function;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.derby.impl.job.scheduler.SchedulerTracer;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactorControl;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

import com.splicemachine.derby.hbase.SpliceDerbyCoprocessor;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexManagementEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import com.splicemachine.derby.hbase.SpliceMasterObserver;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import org.apache.hadoop.hbase.NotServingRegionException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class SpliceTestPlatform extends TestConstants {
	protected MiniZooKeeperCluster miniZooKeeperCluster;
	protected MiniHBaseCluster miniHBaseCluster;
	protected MiniHBaseCluster miniHBaseCluster2;
	protected String zookeeperTargetDirectory;
	protected String hbaseTargetDirectory;
    protected Integer masterPort;
    protected Integer masterInfoPort;
    protected Integer regionServerPort;
    protected Integer regionServerInfoPort;

    final Random random = new Random();

    private boolean randomly(int percent) {
        return random.nextInt(100) < percent;
    }

    final Runnable randomWrappedExecutionException = new Runnable() {
        @Override
        public void run() {
            if (randomly(12)) {
                throw new RuntimeException(new ExecutionException("invalidating on purpose", new NotServingRegionException()));
            }
        }
    };

    final Runnable randomRuntimeException = new Runnable() {
        @Override
        public void run() {
            if (randomly(0)) {
                throw new RuntimeException("failing on purpose");
            }
        }
    };

    final Function<TransactionId, Object> randomTransactionFail = new Function<TransactionId, Object>() {
        @Override
        public Object apply(@Nullable TransactionId transactionId) {
            if (randomly(12)) {
                final TransactorControl transactor = HTransactorFactory.getTransactorControl();
                try {
                    transactor.fail(transactionId);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        }
    };

    public SpliceTestPlatform() {
		super();
	}
	
	public SpliceTestPlatform(String targetDirectory) {
		this(targetDirectory + "zookeeper",targetDirectory + "hbase");
	}

    public SpliceTestPlatform(String zookeeperTargetDirectory, String hbaseTargetDirectory) {
        this(zookeeperTargetDirectory, hbaseTargetDirectory, null, null, null, null);
    }


	public SpliceTestPlatform(String zookeeperTargetDirectory, String hbaseTargetDirectory, Integer masterPort, Integer masterInfoPort, Integer regionServerPort, Integer regionServerInfoPort) {
		this.zookeeperTargetDirectory = zookeeperTargetDirectory;
		this.hbaseTargetDirectory = hbaseTargetDirectory;

        this.masterPort = masterPort;
        this.masterInfoPort = masterInfoPort;
        this.regionServerPort = regionServerPort;
        this.regionServerInfoPort = regionServerInfoPort;
	}

	public static void main(String[] args) throws Exception {
		SpliceTestPlatform spliceTestPlatform;
		if (args.length == 1) {
			spliceTestPlatform = new SpliceTestPlatform(args[0]);
            spliceTestPlatform.start();
		}else if (args.length == 2) {
			spliceTestPlatform = new SpliceTestPlatform(args[0],args[1]);
			spliceTestPlatform.start();
		}else if (args.length == 6) {
            spliceTestPlatform = new SpliceTestPlatform(args[0], args[1], new Integer(args[2]), new Integer(args[3]), new Integer(args[4]), new Integer(args[5]));
            spliceTestPlatform.start();

        }else{
			System.out.println("Splice TestContext Platform supports one argument providing the target directory" +
					" or two arguments dictating the zookeeper and hbase directory.");
			System.exit(1);
		}
	}
	
	public void start() throws Exception {
        SchedulerTracer.registerTaskStart(randomWrappedExecutionException);
        SchedulerTracer.registerTaskEnd(randomRuntimeException);
        SchedulerTracer.registerTaskCommit(randomTransactionFail);
		Configuration config = HBaseConfiguration.create();
		setBaselineConfigurationParameters(config);
		miniHBaseCluster = new MiniHBaseCluster(config,1,1);
	}

	public void end() throws Exception {
        SchedulerTracer.registerTaskStart(null);
        SchedulerTracer.registerTaskEnd(null);
	}

    private void setInt(Configuration configuration, String property, Integer intProperty){
        if(intProperty != null){
            configuration.setInt(property, intProperty.intValue());
        }
    }

	public void setBaselineConfigurationParameters(Configuration configuration) {
		configuration.set("hbase.rootdir", "file://" + hbaseTargetDirectory);
		configuration.setInt("hbase.rpc.timeout", 120000);
		configuration.setInt("hbase.regionserver.lease.period", 120000);		
		configuration.set("hbase.cluster.distributed", "true");
		configuration.setInt("hbase.balancer.period", 10000);
		configuration.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
		configuration.set("hbase.regionserver.handler.count", "40");
        setInt(configuration, "hbase.master.port", masterPort);
        setInt(configuration, "hbase.master.info.port", masterInfoPort);
        setInt(configuration, "hbase.regionserver.port", regionServerPort);
        setInt(configuration, "hbase.regionserver.info.port", regionServerInfoPort);
        configuration.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, true);
        configuration.setInt("hbase.hstore.blockingStoreFiles", 20);
        configuration.setInt("hbase.hregion.memstore.block.multiplier", 4);
        configuration.setFloat("hbase.store.compaction.ratio", (float) 0.25);
        configuration.setFloat("io.hfile.bloom.error.rate", (float)0.005);
        configuration.setInt("hbase.master.event.waiting.time", 20);
        configuration.setInt("hbase.client.pause", 10);
        configuration.setInt("hbase.master.lease.thread.wakefrequency", 3000);
        configuration.setInt("hbase.server.thread.wakefrequency", 1000);
        configuration.setInt("hbase.regionserver.msginterval", 1000);
        configuration.set("hbase.regionserver.region.split.policy", "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy");
        configuration.setLong(HConstants.HREGION_MAX_FILESIZE, 1024 * 1024 * 1024L);        
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
        configuration.set("hbase.coprocessor.master.classes", SpliceMasterObserver.class.getCanonicalName() + "");
    }

}
