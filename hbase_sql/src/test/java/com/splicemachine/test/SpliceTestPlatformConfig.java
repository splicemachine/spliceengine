/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.test;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import static com.google.common.collect.Lists.transform;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.splicemachine.compactions.SpliceDefaultCompactionPolicy;
import com.splicemachine.hbase.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.compactions.SpliceDefaultCompactor;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import com.splicemachine.si.data.hbase.coprocessor.SIObserver;
import com.splicemachine.si.data.hbase.coprocessor.TxnLifecycleEndpoint;
import com.splicemachine.utils.BlockingProbeEndpoint;

/**
 * HBase configuration for SpliceTestPlatform and SpliceTestClusterParticipant.
 */
class SpliceTestPlatformConfig {

    private static final List<Class<?>> REGION_SERVER_COPROCESSORS = ImmutableList.<Class<?>>of(
            RegionServerLifecycleObserver.class,
            BlockingProbeEndpoint.class
    );

    private static final List<Class<?>> REGION_COPROCESSORS = ImmutableList.<Class<?>>of(
            MemstoreAwareObserver.class,
            SpliceIndexObserver.class,
            SpliceIndexEndpoint.class,
            RegionSizeEndpoint.class,
            TxnLifecycleEndpoint.class,
            SIObserver.class,
            BackupEndpointObserver.class
    );

    private static final List<Class<?>> MASTER_COPROCESSORS = ImmutableList.<Class<?>>of(
            SpliceMasterObserver.class);

    private static final List<Class<?>> HFILE_CLEANERS = ImmutableList.<Class<?>>of(
            SpliceHFileCleaner.class,
            TimeToLiveHFileCleaner.class);

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

        Configuration config = HConfiguration.unwrapDelegate();

        config.set(SQLConfiguration.STORAGE_FACTORY_HOME,hbaseRootDirUri);

        //
        // Coprocessors
        //
        config.set("hbase.coprocessor.regionserver.classes", getRegionServerCoprocessorsAsString());
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
        config.setInt("hbase.master.jmx.port", HConfiguration.DEFAULT_JMX_BIND_PORT); // this is set because the HBase master and regionserver are running on the same machine and in the same JVM
        config.setInt(SQLConfiguration.NETWORK_BIND_PORT, derbyPort);
        config.setClass(DefaultStoreEngine.DEFAULT_COMPACTOR_CLASS_KEY, SpliceDefaultCompactor.class, Compactor.class);
       // config.setClass(ConsistencyControlUtils.MVCC_IMPL, SIMultiVersionConsistencyControl.class, ConsistencyControl.class);
        config.setClass(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY, SpliceDefaultCompactionPolicy.class, CompactionPolicy.class);




        //
        // Networking -- interfaces
        //
        // force use of loop back interface on MacOSX, else don't set it
//        if (System.getProperty("os.name").contains("Mac") ) {
//            String interfaceName = "lo0";
//            config.set("hbase.zookeeper.dns.interface", interfaceName);
//            config.set("hbase.master.dns.interface", interfaceName);
//            config.set("hbase.regionserver.dns.interface", interfaceName);
//        }

        //
        // File System
        //
        config.set("fs.defaultFS", "file:///"); // MapR Hack, tells it local filesystem // fs.default.name is deprecated
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
        config.setDouble("yarn.nodemanager.resource.io-spindles",2.0);
        config.set("fs.default.name", "file:///");
        config.set("yarn.nodemanager.container-executor.class","org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor");

        // Must allow Cygwin instance to config its own rootURI
        if (!"CYGWIN".equals(hbaseRootDirUri)) {
            config.set("hbase.rootdir", hbaseRootDirUri);
        }

        //
        // Threads, timeouts
        //
        config.setLong("hbase.rpc.timeout", MINUTES.toMillis(2));
        config.setLong("hbase.client.scanner.timeout.period", MINUTES.toMillis(2)); // hbase.regionserver.lease.period is deprecated
        config.setLong("hbase.client.operation.timeout", MINUTES.toMillis(2));
        config.setLong("hbase.regionserver.handler.count", 200);
        config.setLong("hbase.regionserver.msginterval", 1000);
        config.setLong("hbase.master.event.waiting.time", 20);
        config.setLong("hbase.master.lease.thread.wakefrequency", SECONDS.toMillis(3));
//        config.setBoolean("hbase.master.loadbalance.bytable",true);
        config.setInt("hbase.balancer.period",5000);

        config.setLong("hbase.server.thread.wakefrequency", SECONDS.toMillis(1));
        config.setLong("hbase.client.pause", 100);

        //
        // Compaction Controls
        //
        config.setLong("hbase.hstore.compaction.min", 5); // min number of eligible files before we compact
        config.setLong("hbase.hstore.compaction.max", 10); // max files to be selected for a single minor compaction
        config.setLong("hbase.hstore.compaction.min.size", 16 * MiB); // store files smaller than this will always be eligible for minor compaction.  HFiles this size or larger are evaluated by hbase.hstore.compaction.ratio to determine if they are eligible
        config.setLong("hbase.hstore.compaction.max.size", 248 * MiB); // store files larger than this will be excluded from compaction
        config.setFloat("hbase.hstore.compaction.ratio", 1.25f); // default is 1.2f, at one point we had this set to 0.25f and 25f (which was likely a typo)

        //
        // Memstore, store files, splits
        //
        config.setLong(HConstants.HREGION_MAX_FILESIZE, 32 * MiB); // hbase.hregion.max.filesize
        config.setLong("hbase.hregion.memstore.flush.size", 128 * MiB); // was 512 MiB
        config.setLong("hbase.hregion.memstore.block.multiplier", 4);
        config.setFloat("hbase.regionserver.global.memstore.size", 0.25f); // set mem store to 25% of heap
        config.setLong("hbase.hstore.blockingStoreFiles", 20);
//        config.set("hbase.regionserver.region.split.policy", "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy"); // change default split policy.  this makes more sense for a standalone/single regionserver

        // Support SI
        //config.setClass(HConstants.MVCC_IMPL, SIMultiVersionConsistencyControl.class, ConsistencyControl.class);

        //
        // HFile
        //
        config.setInt("hfile.index.block.max.size", 16 * 1024); // 16KiB
        config.setFloat("hfile.block.cache.size", 0.25f); // set block cache to 25% of heap
        config.setFloat("io.hfile.bloom.error.rate", (float) 0.005);
        config.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, true); // hfile.block.bloom.cacheonwrite
        //config.set("hbase.master.hfilecleaner.plugins", getHFileCleanerAsString());
        config.set("hbase.master.hfilecleaner.plugins", getHFileCleanerAsString());
        //
        // Misc
        //
        config.set("hbase.cluster.distributed", "true");  // don't start zookeeper for us
        config.set("hbase.master.distributed.log.splitting", "false"); // TODO: explain why we are setting this

        //
        // Splice
        //

        config.setLong("splice.ddl.drainingWait.maximum", SECONDS.toMillis(15)); // wait 15 seconds before bailing on bad ddl statements
        config.setLong("splice.ddl.maxWaitSeconds",120000);
        //
        // Snapshots
        //
        config.setBoolean("hbase.snapshot.enabled", true);

        HConfiguration.reloadConfiguration(config);
        return HConfiguration.unwrapDelegate();
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private static final long MiB = 1024L * 1024L;

    private static final Function<Class, String> CLASS_NAME_FUNC = new Function<Class, String>() {
        @Override
        public String apply(Class input) {
            return input.getCanonicalName();
        }
    };

    private static String getRegionServerCoprocessorsAsString() {
        return Joiner.on(",").join(transform(REGION_SERVER_COPROCESSORS, CLASS_NAME_FUNC));
    }

    private static String getRegionCoprocessorsAsString() {
        return Joiner.on(",").join(transform(REGION_COPROCESSORS, CLASS_NAME_FUNC));
    }

    private static String getMasterCoprocessorsAsString() {
        return Joiner.on(",").join(transform(MASTER_COPROCESSORS, CLASS_NAME_FUNC));
    }

    private static String getHFileCleanerAsString() {
        return Joiner.on(",").join(transform(HFILE_CLEANERS, CLASS_NAME_FUNC));
    }
}
