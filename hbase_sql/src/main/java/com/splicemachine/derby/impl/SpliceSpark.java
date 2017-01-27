/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl;

import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDDOperationScope;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.hbase.HBasePipelineEnvironment;
import com.splicemachine.derby.lifecycle.DistributedDerbyStartup;
import com.splicemachine.derby.lifecycle.EngineLifecycleService;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.hbase.RegionServerLifecycleObserver;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.pipeline.ContextFactoryDriverService;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;

public class SpliceSpark {
    private static Logger LOG = Logger.getLogger(SpliceSpark.class);
    static JavaSparkContext ctx;
    static SparkSession session;
    static boolean initialized = false;
    static boolean spliceStaticComponentsSetup = false;
    private static final String SCOPE_KEY = "spark.rdd.scope";
    private static final String SCOPE_OVERRIDE = "spark.rdd.scope.noOverride";
    private static final String OLD_SCOPE_KEY = "spark.rdd.scope.old";
    private static final String OLD_SCOPE_OVERRIDE = "spark.rdd.scope.noOverride.old";

    // Sets both ctx and session
    public static synchronized SparkSession getSession() {
        if (!initialized) {
            session = initializeSparkSession();
            ctx =  new JavaSparkContext(session.sparkContext());
            initialized = true;
        } else if (session.sparkContext().isStopped()) {
            LOG.warn("SparkContext is stopped, reinitializing...");
            session = initializeSparkSession();
            ctx =  new JavaSparkContext(session.sparkContext());
        }
        return session;
    }



    public static synchronized JavaSparkContext getContext() {
        SparkSession s = getSession();
        return ctx;
    }

    public static synchronized boolean isRunningOnSpark() {
        // TODO: This is temporary and is the integrated equivalent of
        // SpliceBaseDerbyCoprocessor.runningOnSpark on master_dataset.
        return !RegionServerLifecycleObserver.isHbaseJVM;
    }

    public static synchronized void setupSpliceStaticComponents() throws IOException {
        try {
            if (!spliceStaticComponentsSetup && isRunningOnSpark()) {
                SynchronousReadResolver.DISABLED_ROLLFORWARD = true;

                //boot SI components
                HBaseSIEnvironment env=HBaseSIEnvironment.loadEnvironment(new SystemClock(),ZkUtils.getRecoverableZooKeeper());
                SIDriver driver = env.getSIDriver();

                //make sure the configuration is correct
                SConfiguration config=driver.getConfiguration();
                //boot derby components
                new EngineLifecycleService(new DistributedDerbyStartup(){
                    @Override public void distributedStart() throws IOException{ }
                    @Override public void markBootFinished() throws IOException{ }
                    @Override public boolean connectAsFirstTime(){ return false; }
                },config).start();

                EngineDriver engineDriver = EngineDriver.driver();
                assert engineDriver!=null: "Not booted yet!";

                //boot the pipeline components
                final Clock clock = driver.getClock();
                ContextFactoryDriver cfDriver =ContextFactoryDriverService.loadDriver();
                //we specify rsServices = null here because we don't actually use the receiving side of the Pipeline environment
                HBasePipelineEnvironment pipelineEnv=HBasePipelineEnvironment.loadEnvironment(clock,cfDriver);
                PipelineDriver.loadDriver(pipelineEnv);
                HBaseRegionLoads.INSTANCE.startWatching();

                spliceStaticComponentsSetup = true;
            }
        } catch (RuntimeException e) {
            LOG.error("Unexpected error setting up Splice components", e);
            throw e;
        } catch (Throwable e) {
            LOG.error("Unexpected error setting up Splice components", e);
            throw new RuntimeException(e);
        }
    }

    private static SparkSession initializeSparkSession() {

        String master = System.getProperty("splice.spark.master", "local[8,4]"); // 8 parallelism, 4 maxFailures
        String sparkHome = System.getProperty("splice.spark.home", null);

        LOG.warn("##############################################################################");
        LOG.warn("    Initializing Spark with: master = " + master);
        LOG.warn("##############################################################################");

        SparkConf conf = new SparkConf();

        // Set default warehouse directory, currently spark can get confused.
        // User Supplied splice.spark.sql.warehouse.dir will overwrite it.
        conf.set("spark.sql.warehouse.dir","/user/splice/spark-warehouse");

        String schedulerAllocationFile = System.getProperty("splice.spark.scheduler.allocation.file");
        if (schedulerAllocationFile != null) {
            conf.set("spark.scheduler.allocation.file", schedulerAllocationFile);
        }
        conf.set("executor.source.splice-machine.class", "com.splicemachine.derby.stream.spark.SpliceMachineSource");
        conf.set("driver.source.splice-machine.class", "com.splicemachine.derby.stream.spark.SpliceMachineSource");

        // pull out Kerberos and set Yarn properties
        if((HConfiguration.unwrapDelegate().get("hbase.master.kerberos.principal") != null) ||
           (HConfiguration.unwrapDelegate().get("hbase.regionserver.kerberos.principal") != null)){
            if(HConfiguration.unwrapDelegate().get("hbase.master.kerberos.principal") != null){
                conf.set("spark.yarn.principal", HConfiguration.unwrapDelegate().get("hbase.master.kerberos.principal"));
            } else if(HConfiguration.unwrapDelegate().get("hbase.regionserver.kerberos.principal") != null){
                conf.set("spark.yarn.principal", HConfiguration.unwrapDelegate().get("hbase.regionserver.kerberos.principal"));
            }
        }

        if((HConfiguration.unwrapDelegate().get("hbase.master.keytab.file") != null) ||
           (HConfiguration.unwrapDelegate().get("hbase.regionserver.keytab.file") != null)){
            if(HConfiguration.unwrapDelegate().get("hbase.master.keytab.file") != null){
                conf.set("spark.yarn.keytab", HConfiguration.unwrapDelegate().get("hbase.master.keytab.file"));
            } else if(HConfiguration.unwrapDelegate().get("hbase.regionserver.keytab.file") != null){
                conf.set("spark.yarn.keytab", HConfiguration.unwrapDelegate().get("hbase.regionserver.keytab.file"));
            }
        }

        // set all spark props that start with "splice.".  overrides are set below.
        for (Object sysPropertyKey : System.getProperties().keySet()) {
            String spsPropertyName = (String) sysPropertyKey;
            if (spsPropertyName.startsWith("splice.spark")) {
                String sysPropertyValue = System.getProperty(spsPropertyName);
                if (sysPropertyValue != null) {
                    String sparkKey = spsPropertyName.replaceAll("^splice\\.", "");
                    conf.set(sparkKey, sysPropertyValue);
                }
            }
        }
        //
        // Our spark property defaults/overrides
        //

        // TODO can this be set/overridden fwith system property, why do we use SpliceConstants?
        conf.set("spark.io.compression.codec",HConfiguration.getConfiguration().getSparkIoCompressionCodec());

         /*
            Application Properties
         */
        conf.set("spark.app.name", System.getProperty("splice.spark.app.name", "SpliceMachine"));
        conf.set("spark.driver.maxResultSize", System.getProperty("splice.spark.driver.maxResultSize", "1g"));
        conf.set("spark.driver.memory", System.getProperty("splice.spark.driver.memory", "1g"));
        conf.set("spark.executor.memory", System.getProperty("splice.spark.executor.memory", "2g"));
        conf.set("spark.extraListeners", System.getProperty("splice.spark.extraListeners", ""));
        conf.set("spark.local.dir", System.getProperty("splice.spark.local.dir", System.getProperty("java.io.tmpdir")));
        conf.set("spark.logConf", System.getProperty("splice.spark.logConf", "true"));
        conf.set("spark.master", master);

        if (master.startsWith("local[8]")) {
            conf.set("spark.cores.max", "8");
        } else if (sparkHome != null) {
            conf.setSparkHome(sparkHome);
        }
        /*
            Spark Streaming
        */
        conf.set("spark.streaming.backpressure.enabled", System.getProperty("splice.spark.streaming.backpressure.enabled", "false"));
        conf.set("spark.streaming.blockInterval", System.getProperty("splice.spark.streaming.blockInterval", "200ms"));
        conf.set("spark.streaming.receiver.maxRate", System.getProperty("splice.spark.streaming.receiver.maxRate", "100"));
        conf.set("spark.streaming.receiver.writeAheadLog.enable", System.getProperty("splice.spark.streaming.receiver.writeAheadLog.enable", "false"));
        conf.set("spark.streaming.unpersist", System.getProperty("splice.spark.streaming.unpersist", "true"));
        conf.set("spark.streaming.kafka.maxRatePerPartition", System.getProperty("splice.spark.streaming.kafka.maxRatePerPartition", ""));
        conf.set("spark.streaming.kafka.maxRetries", System.getProperty("splice.spark.streaming.kafka.maxRetries", "1"));
        conf.set("spark.streaming.ui.retainedBatches", System.getProperty("splice.spark.streaming.ui.retainedBatches", "100"));


        /*

           Spark UI

         */

        conf.set("spark.ui.retainedJobs", System.getProperty("splice.spark.ui.retainedJobs", "100"));
        conf.set("spark.ui.retainedStages", System.getProperty("splice.spark.ui.retainedStages", "100"));
        conf.set("spark.worker.ui.retainedExecutors", System.getProperty("splice.spark.worker.ui.retainedExecutors", "100"));
        conf.set("spark.worker.ui.retainedDrivers", System.getProperty("splice.spark.worker.ui.retainedDrivers", "100"));
        conf.set("spark.ui.retainedJobs", System.getProperty("splice.spark.ui.retainedJobs", "100"));


        if (LOG.isDebugEnabled()) {
            printConfigProps(conf);
        }

        return SparkSession.builder()
                .appName("Splice Spark Session")
                .config(conf)
                .getOrCreate();
    }

    private static void printConfigProps(SparkConf conf) {
        for (Tuple2<String, String> configProp : conf.getAll()) {
            LOG.debug("Spark Prop: "+configProp._1()+" "+configProp._2());
        }
    }

    public static void pushScope(String displayString) {
        JavaSparkContext jspc = SpliceSpark.getContext();
        jspc.setCallSite(displayString);
        jspc.setLocalProperty(OLD_SCOPE_KEY,jspc.getLocalProperty(SCOPE_KEY));
        jspc.setLocalProperty(OLD_SCOPE_OVERRIDE,jspc.getLocalProperty(SCOPE_OVERRIDE));
        jspc.setLocalProperty(SCOPE_KEY,new RDDOperationScope(displayString, null, RDDOperationScope.nextScopeId()+"").toJson());
        jspc.setLocalProperty(SCOPE_OVERRIDE,"true");
    }

    public static void popScope() {
        SpliceSpark.getContext().setLocalProperty("spark.rdd.scope", null);
        SpliceSpark.getContext().setLocalProperty("spark.rdd.scope.noOverride", null);
    }
}
