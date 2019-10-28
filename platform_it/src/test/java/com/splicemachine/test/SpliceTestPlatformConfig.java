/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.test;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.configuration.OlapConfigurations;
import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.compactions.SpliceDefaultCompactionPolicy;
import com.splicemachine.compactions.SpliceDefaultCompactor;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import com.splicemachine.hbase.*;
import com.splicemachine.si.data.hbase.coprocessor.*;
import com.splicemachine.utils.BlockingProbeEndpoint;
import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.token.TokenProvider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.transform;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import com.splicemachine.si.data.hbase.coprocessor.SIObserver;
import com.splicemachine.si.data.hbase.coprocessor.TxnLifecycleEndpoint;
import com.splicemachine.utils.BlockingProbeEndpoint;
import com.splicemachine.hbase.SpliceReplicationService;
/**
 * HBase configuration for SpliceTestPlatform and SpliceTestClusterParticipant.
 */
class SpliceTestPlatformConfig {

    private static final List<Class<?>> REGION_SERVER_COPROCESSORS = ImmutableList.<Class<?>>of(
            RegionServerLifecycleObserver.class,
            BlockingProbeEndpoint.class,
            SpliceReplicationService.class,
            SpliceRSRpcServices.class
    );

    private static final List<Class<?>> REGION_COPROCESSORS = ImmutableList.<Class<?>>of(
            MemstoreAwareObserver.class,
            SpliceIndexObserver.class,
            SpliceIndexEndpoint.class,
            RegionSizeEndpoint.class,
            TxnLifecycleEndpoint.class,
            SIObserver.class,
            BackupEndpointObserver.class ,
            TokenProvider.class
    );


    private static final List<Class<?>> SECURE_REGION_COPROCESSORS = ListUtils.union(ImmutableList.<Class<?>>of(
            TokenProvider.class,
            AccessController.class
    ), REGION_COPROCESSORS);


    private static final List<Class<?>> MASTER_COPROCESSORS = ImmutableList.<Class<?>>of(
            SpliceMasterObserver.class);

    private static final List<Class<?>> SECURE_MASTER_COPROCESSORS = ListUtils.union(
            ImmutableList.<Class<?>>of(
            AccessController.class), MASTER_COPROCESSORS);

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
                                       boolean failTasksRandomly,
                                       String olapLog4jConfig,
                                       boolean secure) {

        Configuration config = HConfiguration.unwrapDelegate();

        config.set(SQLConfiguration.STORAGE_FACTORY_HOME,hbaseRootDirUri);

        //
        // Coprocessors
        //
        config.set("hbase.coprocessor.regionserver.classes", getRegionServerCoprocessorsAsString());
        config.set("hbase.coprocessor.region.classes", getRegionCoprocessorsAsString(secure));
        config.set("hbase.coprocessor.master.classes", getMasterCoprocessorsAsString(secure));

        // Security

        if (secure) {
            String keytab = hbaseRootDirUri + "/splice.keytab";
            config.set("hadoop.security.authentication", "kerberos");
            config.set("hbase.security.authentication", "kerberos");
            config.set("hbase.regionserver.kerberos.principal", "hbase/example.com@EXAMPLE.COM");
            config.set("hbase.regionserver.keytab.file", keytab);
            config.set("hbase.master.kerberos.principal", "hbase/example.com@EXAMPLE.COM");
            config.set("hbase.master.keytab.file", keytab);
            config.set("yarn.nodemanager.principal", "yarn/example.com@EXAMPLE.COM");
            config.set("yarn.resourcemanager.principal", "yarn/example.com@EXAMPLE.COM");
            //read ee_key from a resource file
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            try (InputStream is = classloader.getResourceAsStream("ee.txt")) {
                if (is != null) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                    String eeKey = reader.lines().collect(Collectors.joining(System.lineSeparator()));
                    config.set("splicemachine.enterprise.key", eeKey);
                    config.set("splice.authentication", "LDAP");
                    config.set("splice.authentication.ldap.server", "ldap://localhost:4016");
                    config.set("splice.authentication.ldap.searchAuthDN", "uid=admin,ou=system");
                    config.set("splice.authentication.ldap.searchAuth.password", "secret");
                    config.set("splice.authentication.ldap.searchBase", "ou=users,dc=example,dc=com");
                    config.set("splice.authentication.ldap.searchFilter", "(&(objectClass=inetOrgPerson)(uid=%USERNAME%))");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


        }

//        UserGroupInformation.setLoginUser(Us);

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
        config.setStrings(OlapConfigurations.OLAP_LOG4J_CONFIG, olapLog4jConfig);




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
        String defaultFs = secure ? "hdfs://localhost:58878/" : "file:///";

        config.set("fs.defaultFS", defaultFs); // MapR Hack, tells it local filesystem // fs.default.name is deprecated
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, defaultFs);
        config.setDouble("yarn.nodemanager.resource.io-spindles",2.0);
        config.set("fs.default.name", defaultFs);
        config.set("yarn.nodemanager.container-executor.class","org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor");

        // Must allow Cygwin instance to config its own rootURI
        if (!"CYGWIN".equals(hbaseRootDirUri)) {
            if (secure)
                config.set("hbase.rootdir", "hdfs://localhost:58878/hbase");
            else
                config.set("hbase.rootdir", hbaseRootDirUri+"/hbase");

        }


        //config.set("fs.maprfs.impl","org.apache.hadoop.fs.LocalFileSystem");
        //config.set("fs.hdfs.impl","org.apache.hadoop.fs.LocalFileSystem");
        //config.set("fs.AbstractFileSystem.splice.impll","com.splicemachine.fs.SpliceFs");

        //
        // Threads, timeouts
        //
        config.setClass("hbase.region.server.rpc.scheduler.factory.class", SpliceRpcSchedulerFactory.class, RpcSchedulerFactory.class);
        config.setLong("hbase.rpc.timeout", MINUTES.toMillis(5));
        config.setInt("hbase.client.max.perserver.tasks",50);
        config.setInt("hbase.client.ipc.pool.size",10);
        config.setInt("hbase.rowlock.wait.duration",0);

        config.setLong("hbase.client.scanner.timeout.period", MINUTES.toMillis(2)); // hbase.regionserver.lease.period is deprecated
        config.setLong("hbase.client.operation.timeout", MINUTES.toMillis(2));
        config.setLong("hbase.regionserver.handler.count", 50);
        config.setLong("hbase.regionserver.metahandler.count", 50);
        config.setInt("hbase.hconnection.threads.max", 128);
        config.setInt("hbase.hconnection.threads.core", 8);
        config.setLong("hbase.hconnection.threads.keepalivetime", 300);
        config.setLong("hbase.regionserver.msginterval", 1000);
        config.setLong("hbase.regionserver.optionalcacheflushinterval", 0); // disable automatic flush, meaningless since our timestamps are arbitrary
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
        config.set("hbase.master.hfilecleaner.plugins", getHFileCleanerAsString());
        config.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);
        //
        // Misc
        //
        config.set("hbase.cluster.distributed", "true");  // don't start zookeeper for us
        config.set("hbase.master.distributed.log.splitting", "false"); // TODO: explain why we are setting this

        //
        // AWS Credentials for test...
        //

        //config.set("presto.s3.access-key","");
        //config.set("presto.s3.secret-key","");
        config.set("fs.s3a.impl",com.splicemachine.fs.s3.PrestoS3FileSystem.class.getCanonicalName());
        config.set("hive.exec.orc.split.strategy","BI");
        config.setInt("io.file.buffer.size",65536);

        //
        // Splice
        //

        config.setLong("splice.ddl.drainingWait.maximum", SECONDS.toMillis(15)); // wait 15 seconds before bailing on bad ddl statements
        config.setLong("splice.ddl.maxWaitSeconds",120000);
        config.setInt("splice.olap_server.memory", 4096);
        config.setInt("splice.authentication.token.renew-interval",120);
        config.set("splice.authentication.impersonation.users", "dgf=splice;splice=*");
        config.setBoolean("splice.authentication.impersonation.enabled", true);
        config.set("splice.authentication.ldap.mapGroupAttr", "jy=splice,dgf=splice");
        config.setInt("splice.txn.completedTxns.cacheSize", 4096);
        // below two parameters are needed to test ranger authorization on standalone system
        // config.set("splice.authorization.scheme", "RANGER");
        // config.set("splice.metadataRestrictionEnabled", "RANGER");
        // config.set("splice.ranger.usersync.username.caseconversion", "LOWER");

        // Get more test coverage of the broadcast join Dataset path, as this is the
        // future of splice OLAP query execution.
        config.setLong("splice.optimizer.broadcastDatasetCostThreshold", -1);


        if (derbyPort > SQLConfiguration.DEFAULT_NETWORK_BIND_PORT) {
            // we are a member, let's ignore transactions for testing
            config.setBoolean("splice.ignore.missing.transactions", true);
        }
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

    private static String getRegionCoprocessorsAsString(boolean secure) {
        return Joiner.on(",").join(transform(secure ? SECURE_REGION_COPROCESSORS : REGION_COPROCESSORS, CLASS_NAME_FUNC));
    }

    private static String getMasterCoprocessorsAsString(boolean secure) {
        return Joiner.on(",").join(transform(secure ? SECURE_MASTER_COPROCESSORS : MASTER_COPROCESSORS, CLASS_NAME_FUNC));
    }

    private static String getHFileCleanerAsString() {
        return Joiner.on(",").join(transform(HFILE_CLEANERS, CLASS_NAME_FUNC));
    }
}
