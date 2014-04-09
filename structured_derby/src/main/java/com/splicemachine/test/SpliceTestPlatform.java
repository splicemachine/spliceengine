package com.splicemachine.test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.log4j.Logger;

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
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.impl.TransactionId;

public class SpliceTestPlatform extends TestConstants {
    private static final Logger LOG = Logger.getLogger(SpliceTestPlatform.class);
	protected MiniZooKeeperCluster miniZooKeeperCluster;
	protected MiniHBaseCluster miniHBaseCluster;
	protected MiniHBaseCluster miniHBaseCluster2;
	protected String zookeeperTargetDirectory;
	protected String hbaseRootDirUri;
    protected Integer masterPort;
    protected Integer masterInfoPort;
    protected Integer regionServerPort;
    protected Integer regionServerInfoPort;
    protected Integer derbyPort = SpliceConstants.DEFAULT_DERBY_BIND_PORT;
    protected boolean failTasksRandomly;
    
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
                final TransactionManager transactor = HTransactorFactory.getTransactionManager();
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

    public SpliceTestPlatform(String zookeeperTargetDirectory, String hbaseRootDirUri, String failTasksRandomly) {
        this(zookeeperTargetDirectory, hbaseRootDirUri, null, null, null, null, failTasksRandomly);
    }


	public SpliceTestPlatform(String zookeeperTargetDirectory, String hbaseRootDirUri, Integer masterPort, Integer masterInfoPort, Integer regionServerPort, Integer regionServerInfoPort, String failTasksRandomly) {
		this.zookeeperTargetDirectory = zookeeperTargetDirectory;
		this.hbaseRootDirUri = hbaseRootDirUri;

        this.masterPort = masterPort;
        this.masterInfoPort = masterInfoPort;
        this.regionServerPort = regionServerPort;
        this.regionServerInfoPort = regionServerInfoPort;
        this.failTasksRandomly = (failTasksRandomly.compareToIgnoreCase("TRUE") == 0);
	}

    public SpliceTestPlatform(String zookeeperTargetDirectory, String hbaseRootDirUri, Integer masterPort,
                              Integer masterInfoPort, Integer regionServerPort, Integer regionServerInfoPort, Integer derbyPort, String failTasksRandomly) {
        this(zookeeperTargetDirectory, hbaseRootDirUri, masterPort,
                masterInfoPort, regionServerPort, regionServerInfoPort, failTasksRandomly);
        this.derbyPort = derbyPort;
    }

	public static void main(String[] args) throws Exception {
		
		SpliceTestPlatform spliceTestPlatform;
		if (args.length == 3) {
			spliceTestPlatform = new SpliceTestPlatform(args[0],args[1],args[2]);
			spliceTestPlatform.start();
		}else if (args.length == 7) {
            spliceTestPlatform = new SpliceTestPlatform(args[0], args[1], new Integer(args[2]), new Integer(args[3]), new Integer(args[4]), new Integer(args[5]), args[6]);
            spliceTestPlatform.start();

        }else if (args.length == 8) {
            spliceTestPlatform = new SpliceTestPlatform(args[0], args[1], new Integer(args[2]), new Integer(args[3]), new Integer(args[4]), new Integer(args[5]), new Integer(args[6]), args[7]);
            spliceTestPlatform.start();

        }else{
			System.out.println("Splice TestContext Platform supports arguments indicating the zookeeper target "+
                    "directory, hbase directory URI and a boolean flag indicating whether the chaos monkey should randomly fail tasks.");
			System.exit(1);
		}
	}
	
	public void start() throws Exception {
		Configuration config = HBaseConfiguration.create();
		setBaselineConfigurationParameters(config);
        LOG.info("Derby listen port: "+derbyPort);
        LOG.info("Random task failure "+(this.failTasksRandomly?"WILL":"WILL NOT")+" occur");
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
		configuration.set("hbase.rootdir", hbaseRootDirUri);
		configuration.setInt("hbase.rpc.timeout", 120000);
		configuration.setInt("hbase.client.scanner.timeout.period", 120000);
		configuration.set("hbase.cluster.distributed", "true");
		configuration.setInt("hbase.balancer.period", 10000);
		configuration.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
		configuration.set("hbase.regionserver.handler.count", "200");
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
        configuration.setFloat("hbase.store.compaction.ratio", 025f);
        configuration.setInt("hbase.hstore.compaction.min", 5);
        configuration.setLong("hbase.hregion.memstore.flush.size", 512 * 1024 * 1024L);
        configuration.setInt("hbase.hstore.compaction.max", 10);
        configuration.setLong("hbase.hstore.compaction.min.size", 16 * 1024 * 1024L);
        configuration.setLong("hbase.hstore.compaction.max.size", 248 * 1024 * 1024L);        
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
        configuration.setLong(HConstants.HREGION_MAX_FILESIZE, 1024 * 1024 * 1024L);

        //set a low value threshold for gz file size on import
        configuration.setLong(SpliceConstants.SEQUENTIAL_IMPORT_FILESIZE_THREASHOLD, 1024 * 1024L);
        //set a random task failure rate
        configuration.set(SpliceConstants.DEBUG_TASK_FAILURE_RATE,Double.toString(0.05d));
        if (failTasksRandomly) {
        	configuration.set(SpliceConstants.DEBUG_FAIL_TASKS_RANDOMLY,"true");
        }
        
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
