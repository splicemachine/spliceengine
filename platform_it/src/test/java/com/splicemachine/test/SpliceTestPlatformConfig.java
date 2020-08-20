/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static com.google.common.collect.Lists.transform;

import java.util.List;

import com.splicemachine.access.configuration.OlapConfigurations;
import com.splicemachine.compactions.SpliceDefaultCompactionPolicy;
import com.splicemachine.compactions.SpliceDefaultCompactor;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.token.TokenProvider;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.splicemachine.hbase.*;
import com.splicemachine.utils.BlockingProbeEndpoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.stream.Collectors;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.si.data.hbase.coprocessor.SIObserver;
import com.splicemachine.si.data.hbase.coprocessor.TxnLifecycleEndpoint;
import com.splicemachine.hbase.SpliceRSRpcServices;
/**
 * HBase configuration for SpliceTestPlatform and SpliceTestClusterParticipant.
 */
class SpliceTestPlatformConfig {

    private static final List<Class<?>> REGION_SERVER_COPROCESSORS = ImmutableList.<Class<?>>of(
            RegionServerLifecycleObserver.class,
            BlockingProbeEndpoint.class,
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

    public static void configureS3(Configuration config)
    {
        // AWS Credentials for test
        splice.aws.com.amazonaws.auth.AWSCredentialsProvider credentialproviders[] = {
                new splice.aws.com.amazonaws.auth.EnvironmentVariableCredentialsProvider(), // first try env AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
                new splice.aws.com.amazonaws.auth.profile.ProfileCredentialsProvider()              // second try from $HOME/.aws/credentials (aws cli store)
        };
        for( splice.aws.com.amazonaws.auth.AWSCredentialsProvider provider : credentialproviders )
        {
            try {
                // can throw SdkClientException if env is not set or can't parse .aws/credentials
                // or IllegalArgumentException if .aws/credentials is not there
                splice.aws.com.amazonaws.auth.AWSCredentials cred = provider.getCredentials();
                config.set("presto.s3.access-key", cred.getAWSAccessKeyId());
                config.set("presto.s3.secret-key", cred.getAWSSecretKey());
                config.set("fs.s3a.access.key", cred.getAWSAccessKeyId());
                config.set("fs.s3a.secret.key", cred.getAWSSecretKey());
                config.set("fs.s3a.awsAccessKeyId", cred.getAWSAccessKeyId());
                config.set("fs.s3a.awsSecretAccessKey", cred.getAWSSecretKey());
                break;
            } catch( Exception e )
            {
                continue;
            }
        }
        config.set("fs.s3a.impl", com.splicemachine.fs.s3.PrestoS3FileSystem.class.getCanonicalName() );
    }
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
        //config.setClass("hbase.region.server.rpc.scheduler.factory.class", SpliceRpcSchedulerFactory.class, RpcSchedulerFactory.class);
        config.setLong("hbase.rpc.timeout", MINUTES.toMillis(5));
        config.setInt("hbase.client.max.perserver.tasks",50);
        config.setInt("hbase.client.ipc.pool.size",10);
        config.setInt("hbase.client.retries.number", 35);
        config.setInt("hbase.rowlock.wait.duration",10);

        config.setLong("hbase.client.scanner.timeout.period", MINUTES.toMillis(2)); // hbase.regionserver.lease.period is deprecated
        config.setLong("hbase.client.operation.timeout", MINUTES.toMillis(2));
        config.setLong("hbase.regionserver.handler.count", 50);
        config.setLong("hbase.regionserver.metahandler.count", 50);
        config.setInt("hbase.hconnection.threads.max", 128);
        config.setInt("hbase.hconnection.threads.core", 8);
        config.setLong("hbase.hconnection.threads.keepalivetime", 300);
        config.setLong("hbase.regionserver.msginterval", 15000);
        config.setLong("hbase.regionserver.optionalcacheflushinterval", 0); // disable automatic flush, meaningless since our timestamps are arbitrary
        config.setLong("hbase.master.event.waiting.time", 20);
        config.setLong("hbase.master.lease.thread.wakefrequency", SECONDS.toMillis(3));
//        config.setBoolean("hbase.master.loadbalance.bytable",true);
        config.setInt("hbase.hfile.compaction.discharger.interval", 20*1000);
        // The hbase balancer uses a lot of memory and network resources.
        // Effectively disable this on standalone to avoid OOM and network hiccups.
        config.setInt("hbase.balancer.period",300000000); // 5000 minutes
        config.setInt("hbase.balancer.statusPeriod",300000000); // 5000 minutes

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
        config.set("hbase.procedure.store.wal.use.hsync", "false");
        config.set("hbase.unsafe.stream.capability.enforce", "false");
        //
        // Misc
        //
        config.set("hbase.regionserver.enable.table.latencies", "false"); // disable table latencies, memory intensive
        config.set("hbase.cluster.distributed", "true");  // don't start zookeeper for us
        config.set("hbase.master.distributed.log.splitting", "false"); // TODO: explain why we are setting this

        configureS3( config );

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
        //config.set("splice.replication.monitor.quorum", "srv091:2181");
        //config.set("splice.replication.healthcheck.script", "/Users/jyuan/replication/scripts/healthCheck.sh");

        // below two parameters are needed to test ranger authorization on standalone system
        // config.set("splice.authorization.scheme", "RANGER");
        // config.set("splice.metadataRestrictionEnabled", "RANGER");
        // config.set("splice.ranger.usersync.username.caseconversion", "LOWER");

        // Get more test coverage of the broadcast join Dataset path, as this is the
        // future of splice OLAP query execution.
        config.setLong("splice.optimizer.broadcastDatasetCostThreshold", -1);

        // Fix SessionPropertyIT.TestTableLimitForExhaustiveSearchSessionProperty
        // by setting a min query planner timeout of 5 ms.  Otherwise, with small
        // tables, we may timeout too quickly when the system is a little busy
        // and get an unexpected join plan.
        config.setLong("splice.optimizer.minPlanTimeout", 5L);

        if (derbyPort > SQLConfiguration.DEFAULT_NETWORK_BIND_PORT) {
            // we are a member, let's ignore transactions for testing
            config.setBoolean("splice.ignore.missing.transactions", true);
        }
        //
        // Snapshots
        //
        config.setBoolean("hbase.snapshot.enabled", true);


        //
        // Replication
        //
        config.setBoolean("replication.source.eof.autorecovery", true);
        //config.set("hbase.replication.source.service", "com.splicemachine.replication.SpliceReplication");
        //config.set("hbase.replication.sink.service", "com.splicemachine.replication.SpliceReplication");
        config.setBoolean("replication.source.eof.autorecovery", true);
        config.setBoolean("splice.replication.enabled", true);

        //
        // Spark configuration
        //
        Properties properties = System.getProperties();
        String buildDir = properties.getProperty("projectBuildDirectory");
        String osDetectedClassifier = properties.getProperty("osDetectedClassifier");
        String envClassifier = properties.getProperty("envClassifier");
        String projectBaseDir = properties.getProperty("projectBaseDir");
        String log4jUrl = properties.getProperty("log4jUrl");
        String jacocoAgent = properties.getProperty("jacocoAgent");
        String userName = properties.getProperty("userName");

        String executorClassPath = String.format("%s:%s/classes:%s/dependency/*", buildDir, buildDir, buildDir);
        String driverClassPath = String.format("%s/classes:%s/dependency/*", buildDir, buildDir);
        String hbaseRoot = buildDir + "/hbase";
        String libPath = buildDir + "/../../assembly/" + envClassifier + "/native/" + osDetectedClassifier;
        String driverJavaOptions = String.format("-Djava.security.krb5.conf=%s/krb5.conf -enableassertions " +
                "-Dhbase.rootdir=%s -Xms64m -XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=50", buildDir, hbaseRoot);
        String yarnJavaOptions = String.format("-Djava.security.krb5.conf=%s/krb5.conf " +
                "-Xms64m -XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=50", buildDir);
        String executorJavaOption=String.format("-Dhbase.rootdir=%s -Dlog4j.configuration=%s " +
                "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4020 -Djava.security.krb5.conf=%s/krb5.conf " +
                "-enableassertions -Xms64m -XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=40 %s",
                hbaseRoot, log4jUrl, buildDir, jacocoAgent);
        String olapServerClassPath = String.format("%s:%s/dependency/javax.servlet-api-3.1.0.jar:%s/classes",
                buildDir, buildDir, buildDir);
        String olapServerJavaOptions = String.format("-Djava.library.path=%s/../../assembly/%s/native/%s:/usr/local/lib/ " +
                        "-Dhbase.rootdir=%s -Dlog4j.configuration=%s -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4025 " +
                        "-enableassertions -Djava.net.preferIPv4Stack=true -Djava.security.krb5.conf=%s/krb5.conf " +
                        "-Xms64m -XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=30",
                buildDir, envClassifier, osDetectedClassifier, hbaseRoot, log4jUrl, buildDir);

        String yarnJars = String.format("local:%s/dependency/*", buildDir);
        String warehouseDir = String.format("%s/spark-warehouse", buildDir);

        config.set("splice.spark.app.name", "SpliceMachine");
        config.setBoolean("splice.spark.eventLog.enabled", true);
        config.set("splice.spark.eventLog.dir", buildDir+"/SpliceTestYarnPlatform");
        config.set("splice.spark.driver.host", "localhost");
        config.set("splice.spark.sql.warehouse.dir", warehouseDir);
        config.setInt("splice.spark.driver.cores", 1);
        config.set("splice.spark.driver.extraClassPath", driverClassPath);
        config.set("splice.spark.executor.extraClassPath", executorClassPath);
        config.set("splice.spark.driver.maxResultSize", "1g");
        config.set("splice.spark.driver.memory", "1g");
        config.setBoolean("splice.spark.enabled", true);
        config.setInt("splice.spark.executor.cores", 3);
        config.set("spark.executor.extraLibraryPath",libPath);
        config.setInt("splice.spark.executor.instances", 1);
        config.set("splice.spark.executor.memory", "2000M");
        config.set("splice.spark.io.compression.lz4.blockSize", "32k");
        config.setBoolean("spark.broadcast.compress", true);
        config.setBoolean("splice.spark.kryo.referenceTracking", false);
        config.set("splice.spark.kryo.registrator", "com.splicemachine.derby.impl.SpliceSparkKryoRegistrator");
        config.set("splice.spark.kryoserializer.buffer.max", "512m");
        config.set("splice.spark.kryoserializer.buffer", "4m");
        config.set("splice.spark.locality.wait", "60s");
        config.setBoolean("splice.spark.logConf", true);
        config.set("splice.spark.master", "yarn-client");
        config.set("splice.spark.scheduler.allocation.file", projectBaseDir + "/src/main/resources/fairscheduler.xml");
        config.set("splice.spark.scheduler.mode", "FAIR");
        config.setBoolean("splice.spark.blacklist.enabled", false);
        config.set("splice.spark.serializer", "com.splicemachine.serializer.SpliceKryoSerializer");
        config.set("splice.spark.driver.extraJavaOptions", driverJavaOptions);
        config.set("splice.spark.yarn.am.extraJavaOptions", yarnJavaOptions);
        config.set("splice.spark.executor.extraJavaOptions", executorJavaOption);
        if (userName != null) {
            config.set("splice.spark.yarn.user", userName);
        }
        config.set("splice.olapServer.classpath", olapServerClassPath);
        config.set("splice.olapServer.extraJavaOptions", olapServerJavaOptions);
        config.set("splice.spark.shuffle.file.buffer", "128k");
        config.setFloat("splice.spark.shuffle.memoryFraction", 0.7f);
        config.setBoolean("splice.spark.shuffle.service.enabled", true);
        config.set("splice.spark.yarn.am.waitTime", "10s");
        config.set("spark.yarn.jars", yarnJars);

        if (secure) {
            String keyTab = String.format("%s/splice.keytab", buildDir);
            config.set("splice.spark.yarn.principal", "hbase/example.com@EXAMPLE.COM");
            config.set("splice.spark.yarn.keytab", keyTab);
            config.set("splice.spark.hadoop.fs.defaultFS", "hdfs://localhost:58878");
        }

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
