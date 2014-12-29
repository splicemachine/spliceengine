package com.splicemachine.test;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.*;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.coprocessors.TimestampMasterObserver;
import com.splicemachine.si.coprocessors.TxnLifecycleEndpoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

import java.util.List;

import static com.google.common.collect.Lists.transform;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * HBase configuration for SpliceTestPlatform and SpliceTestClusterParticipant.
 */
class SpliceTestPlatformConfig {

    private static final List<Class<?>> REGION_COPROCESSORS = ImmutableList.<Class<?>>of(
            SpliceOperationRegionObserver.class,
            SpliceIndexObserver.class,
            SpliceDerbyCoprocessor.class,
            SpliceIndexManagementEndpoint.class,
            SpliceIndexEndpoint.class,
            CoprocessorTaskScheduler.class,
            TxnLifecycleEndpoint.class,
            SIObserver.class);

    private static final List<Class<?>> MASTER_COPROCESSORS = ImmutableList.<Class<?>>of(
            SpliceMasterObserver.class,
            TimestampMasterObserver.class);


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

        //
        // Coprocessors
        //
        config.set("hbase.coprocessor.region.classes", getRegionCoprocessorsAsString());
        config.set("hbase.coprocessor.master.classes", getMasterCoprocessorsAsString());

        //
        // Networking
        //
        config.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
        config.setInt("hbase.master.port", masterPort);
        config.setInt("hbase.master.info.port", masterInfoPort);
        config.setInt("hbase.regionserver.port", regionServerPort);
        config.setInt("hbase.regionserver.info.port", regionServerInfoPort);
        config.setInt("hbase.master.jmx.port", 10102);
        config.setInt(SpliceConstants.DERBY_BIND_PORT, derbyPort);

        //
        // Networking -- interfaces
        //
        String interfaceName = System.getProperty("os.name").contains("Mac") ? "lo0" : "default";
        config.set("hbase.zookeeper.dns.interface", interfaceName);
        config.set("hbase.regionserver.dns.interface", interfaceName);
        config.set("hbase.master.dns.interface", interfaceName);

        //
        // File System
        //

        // MapR Hack, tells it local filesystem
        config.set("fs.default.name", "file:///");
        // Must allow Cygwin instance to config its own rootURI
        if (!"CYGWIN".equals(hbaseRootDirUri)) {
            config.set("hbase.rootdir", hbaseRootDirUri);
        }

        //
        // Threads, timeouts
        //
        config.setLong("hbase.rpc.timeout", MINUTES.toMillis(2));
        config.setLong("hbase.regionserver.lease.period", MINUTES.toMillis(2));
        config.setLong("hbase.regionserver.handler.count", 200);
        config.setLong("hbase.regionserver.msginterval", 1000);
        config.setLong("hbase.master.event.waiting.time", 20);
        config.setLong("hbase.master.lease.thread.wakefrequency", SECONDS.toMillis(3));
        config.setLong("hbase.server.thread.wakefrequency", SECONDS.toMillis(1));
        config.setLong("hbase.client.pause", 100);

        //
        // Compaction Controls
        //
        config.setLong("hbase.hstore.compaction.min", 5); // Minimum Number of Files for Compaction
        config.setLong("hbase.hstore.compaction.max", 10);
        config.setLong("hbase.hstore.compaction.min.size", 16 * MiB);
        config.setLong("hbase.hstore.compaction.max.size", 248 * MiB);

        //
        // Memstore, store files, splits
        //
        config.setLong("hbase.hregion.max.filesize", 1024 * MiB);
        config.setLong("hbase.hregion.memstore.flush.size", 512 * MiB); // Way too high
        config.setLong("hbase.hregion.memstore.block.multiplier", 4);
        config.setFloat("hbase.regionserver.global.memstore.size", 0.25f);
        config.setLong("hbase.hstore.blockingStoreFiles", 20);
        config.set("hbase.regionserver.region.split.policy", "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy");

        //
        // HFile
        //
        config.setInt("hfile.index.block.max.size", 16 * 1024); // Aaron
        config.setFloat("hfile.block.cache.size", 0.25f);
        config.setFloat("io.hfile.bloom.error.rate", (float) 0.005);

        //
        // Misc
        //
        config.set("hbase.cluster.distributed", "true");
        config.set("hbase.master.distributed.log.splitting", "false"); // Why?
        config.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, true);

        //
        // Splice
        //

        //wait 15 seconds before bailing on bad ddl statements
        config.setLong("splice.ddl.drainingWait.maximum", SECONDS.toMillis(15));

        config.setDouble(SpliceConstants.DEBUG_TASK_FAILURE_RATE, 0.05d);
        config.setBoolean(SpliceConstants.DEBUG_FAIL_TASKS_RANDOMLY, failTasksRandomly);

        config.reloadConfiguration();
        SIConstants.reloadConfiguration(config);
        return config;
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private static final long MiB = 1024L * 1024L;

    private static final Function<Class, String> CLASS_NAME_FUNC = new Function<Class, String>() {
        @Override
        public String apply(Class input) {
            return input.getCanonicalName();
        }
    };

    private static String getRegionCoprocessorsAsString() {
        return Joiner.on(",").join(transform(REGION_COPROCESSORS, CLASS_NAME_FUNC));
    }

    private static String getMasterCoprocessorsAsString() {
        return Joiner.on(",").join(transform(MASTER_COPROCESSORS, CLASS_NAME_FUNC));
    }

}
