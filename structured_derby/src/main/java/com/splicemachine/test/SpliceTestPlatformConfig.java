package com.splicemachine.test;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.*;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.coprocessors.TimestampMasterObserver;
import com.splicemachine.si.coprocessors.TxnLifecycleEndpoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

import java.util.Arrays;
import java.util.List;
import static java.util.concurrent.TimeUnit.*;

import static com.google.common.collect.Lists.transform;

/**
 * HBase configuration common to both SpliceTestPlatform and SpliceTestClusterParticipant.
 */
class SpliceTestPlatformConfig {

    private static final List<Class<?>> REGION_COPROCESSORS = Arrays.<Class<?>>asList(
            SpliceOperationRegionObserver.class,
            SpliceIndexObserver.class,
            SpliceDerbyCoprocessor.class,
            SpliceIndexManagementEndpoint.class,
            SpliceIndexEndpoint.class,
            CoprocessorTaskScheduler.class,
            TxnLifecycleEndpoint.class,
            SIObserver.class);

    private static final List<Class<?>> MASTER_COPROCESSORS = Arrays.<Class<?>>asList(
            SpliceMasterObserver.class,
            TimestampMasterObserver.class);

    private static final Joiner COMMAS = Joiner.on(",");

    private static final Function<Class, String> CLASS_NAME = new Function<Class, String>() {
        @Override
        public String apply(Class input) {
            return input.getCanonicalName();
        }
    };

    /*
     * Create an HBase config object suitable for use in our test platform.
     */
    public static Configuration create(String hbaseRootDirUri,
                                       Integer masterPort,
                                       Integer masterInfoPort,
                                       Integer regionServerPort,
                                       Integer regionServerInfoPort,
                                       Integer derbyPort,
                                       boolean failTasksRandomly) {

        Configuration config = HBaseConfiguration.create();

        config.set("hbase.rootdir", hbaseRootDirUri);
        config.setLong("hbase.rpc.timeout", MINUTES.toMillis(2));
        config.setLong("hbase.regionserver.lease.period", MINUTES.toMillis(2));
        config.set("hbase.cluster.distributed", "true");
        config.setLong("hbase.balancer.period", SECONDS.toMillis(10));
        config.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
        config.set("hbase.regionserver.handler.count", "200");
        setInt(config, "hbase.master.port", masterPort);
        setInt(config, "hbase.master.info.port", masterInfoPort);
        setInt(config, "hbase.regionserver.port", regionServerPort);
        setInt(config, "hbase.regionserver.info.port", regionServerInfoPort);
        config.setInt(SpliceConstants.DERBY_BIND_PORT, derbyPort);
        config.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, true);
        config.setInt("hbase.hstore.blockingStoreFiles", 20);
        config.setInt("hbase.hregion.memstore.block.multiplier", 4);
        config.setFloat("hbase.store.compaction.ratio", (float) 0.25);
        config.setFloat("io.hfile.bloom.error.rate", (float) 0.005);
        config.setInt("hbase.master.event.waiting.time", 20);
        config.setInt("hbase.client.pause", 100);
        config.setFloat("hbase.store.compaction.ratio", 025f);
        config.setInt("hbase.hstore.compaction.min", 5);
        config.setInt("hfile.index.block.max.size", 16 * 1024);
        config.setLong("hbase.hregion.memstore.flush.size", 512 * 1024 * 1024L);
        config.setInt("hbase.hstore.compaction.max", 10);
        config.setLong("hbase.hstore.compaction.min.size", 16 * 1024 * 1024L);
        config.setLong("hbase.hstore.compaction.max.size", 248 * 1024 * 1024L);
        config.setInt("hbase.master.lease.thread.wakefrequency", 3000);
        config.setInt("hbase.server.thread.wakefrequency", 1000);
        config.setInt("hbase.regionserver.msginterval", 1000);
        config.set("hbase.regionserver.region.split.policy", "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy");
        String interfaceName = System.getProperty("os.name").contains("Mac") ? "lo0" : "default";
        config.set("hbase.zookeeper.dns.interface", interfaceName);
        config.set("hbase.regionserver.dns.interface", interfaceName);
        config.set("hbase.master.dns.interface", interfaceName);
        config.setLong(HConstants.HREGION_MAX_FILESIZE, 1024 * 1024 * 1024L); // 128?

        //set a low value threshold for gz file size on import
        config.setLong(SpliceConstants.SEQUENTIAL_IMPORT_FILESIZE_THREASHOLD, 1024 * 1024L);
        //set a random task failure rate
        config.set(SpliceConstants.DEBUG_TASK_FAILURE_RATE, Double.toString(0.05d));
        config.set(SpliceConstants.DEBUG_FAIL_TASKS_RANDOMLY, String.valueOf(failTasksRandomly));

        config.set("hbase.coprocessor.region.classes", COMMAS.join(transform(REGION_COPROCESSORS, CLASS_NAME)));
        config.set("hbase.coprocessor.master.classes", COMMAS.join(transform(MASTER_COPROCESSORS, CLASS_NAME)));

        config.reloadConfiguration();

        SIConstants.reloadConfiguration(config);

        return config;
    }

    private static void setInt(Configuration configuration, String property, Integer intProperty) {
        if (intProperty != null) {
            configuration.setInt(property, intProperty);
        }
    }


}
