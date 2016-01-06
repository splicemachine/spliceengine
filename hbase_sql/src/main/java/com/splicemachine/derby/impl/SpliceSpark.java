package com.splicemachine.derby.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.db.jdbc.EmbeddedDriver;
import com.splicemachine.derby.stream.spark.SpliceMachineSource;
import com.splicemachine.hbase.RegionServerLifecycleObserver;
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import com.google.common.base.Splitter;
import org.apache.spark.rdd.RDDOperationScope;
import scala.Tuple2;

public class SpliceSpark {
    private static Logger LOG = Logger.getLogger(SpliceSpark.class);
    static JavaSparkContext ctx;
    static boolean initialized = false;
    public static boolean active = true;
    static volatile JavaSparkContext localContext = null;
    static boolean spliceStaticComponentsSetup = false;
    private static final String SCOPE_KEY = "spark.rdd.scope";
    private static final String SCOPE_OVERRIDE = "spark.rdd.scope.noOverride";
    private static final String OLD_SCOPE_KEY = "spark.rdd.scope.old";
    private static final String OLD_SCOPE_OVERRIDE = "spark.rdd.scope.noOverride.old";


    private static String[] getJarFiles(String file) {
        if (file == null || file.isEmpty())
            return new String[0];
        List<String> jars = new ArrayList<String>();
        for (String name : file.split(":")) {
            File dir = new File(name);
            getJarFiles(jars, dir);
        }
        return jars.toArray(new String[0]);
    }

    private static void getJarFiles(List<String> files, File file) {
        if (file.isDirectory()) {
            for (File jar : file.listFiles()) {
                getJarFiles(files, jar);
            }
        } else {
            files.add(file.getAbsolutePath());
        }
    }

    public static synchronized JavaSparkContext getContext() {
        if (!initialized) {
            if (active) {
                ctx = initializeSparkContext();
            } else {
                LOG.warn("Spark not active");
            }
            initialized = true;
        }
        return ctx;
    }

    public static synchronized boolean isRunningOnSpark() {
        // This is temporary and is the integrated equivalent of
        // SpliceBaseDerbyCoprocessor.runningOnSpark on master_dataset.
        return !RegionServerLifecycleObserver.isHbaseJVM;
    }

    public static synchronized void setupSpliceStaticComponents() throws IOException {
        try {
            if (!spliceStaticComponentsSetup && isRunningOnSpark()) {
                SynchronousReadResolver.DISABLED_ROLLFORWARD = true;
                new EmbeddedDriver();
                new SpliceAccessManager();
//                SpliceDriver driver = SpliceDriver.driver();
//                if (!driver.isStarted()) {
//                    driver.start(); // TODO might cause NPEs....
//                }
                EngineDriver driver = EngineDriver.driver();
                assert driver!=null: "Not booted yet!";
//                if(driver!=null){
//                    driver.loadUUIDGenerator(1); // Need to get Spark Port? TODO JL
//                }
//                if (driver.getUUIDGenerator() == null) {
//                }
                spliceStaticComponentsSetup = true;
                SpliceMachineSource.register();
            }
        } catch (RuntimeException e) {
            LOG.error("Unexpected error setting up Splice components", e);
            throw e;
        } catch (Throwable e) {
            LOG.error("Unexpected error setting up Splice components", e);
            throw new RuntimeException(e);
        }
    }

    private static JavaSparkContext initializeSparkContext() {

        String master = System.getProperty("splice.spark.master", "local[8]");
        String sparkHome = System.getProperty("splice.spark.home", null);

        LOG.warn("##############################################################################");
        LOG.warn("    Initializing Spark with: master = " + master);
        LOG.warn("##############################################################################");

        SparkConf conf = new SparkConf();

        String schedulerAllocationFile = System.getProperty("splice.spark.scheduler.allocation.file");
        if (schedulerAllocationFile != null) {
            conf.set("spark.scheduler.allocation.file", schedulerAllocationFile);
        }
        conf.set("executor.source.splice-machine.class", "com.splicemachine.derby.stream.spark.SpliceMachineSource");
        conf.set("driver.source.splice-machine.class", "com.splicemachine.derby.stream.spark.SpliceMachineSource");

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
        conf.set("spark.io.compression.codec",HConfiguration.INSTANCE.unwrapDelegate().get("spark.io.compression.codec","lz4"));

         /*
            Application Properties
         */
        conf.set("spark.app.name", System.getProperty("splice.spark.app.name", "SpliceMachine"));
        conf.set("spark.driver.cores",System.getProperty("splice.spark.driver.cores", "8"));
        conf.set("spark.driver.maxResultSize", System.getProperty("splice.spark.driver.maxResultSize", "1g"));
        conf.set("spark.driver.memory", System.getProperty("splice.spark.driver.memory", "1g"));
        conf.set("spark.executor.memory", System.getProperty("splice.spark.executor.memory", "2g"));
        conf.set("spark.extraListeners", System.getProperty("splice.spark.extraListeners", ""));
        conf.set("spark.local.dir", System.getProperty("splice.spark.local.dir", System.getProperty("java.io.tmpdir")));
        conf.set("spark.logConf", System.getProperty("splice.spark.logConf", "true"));
        conf.set("spark.master", master);

        if (master.startsWith("local[8]")) {
            conf.set("spark.cores.max", "8");
            if (localContext == null) {
                localContext = new JavaSparkContext(conf);
            }
            return localContext;
        } else if (sparkHome != null) {
            conf.setSparkHome(sparkHome);
        }
        /*
            Spark Streaming
        */
        conf.set("spark.streaming.backpressure.enabled", System.getProperty("splice.spark.streaming.backpressure.enabled", "false"));
        conf.set("spark.streaming.blockInterval", System.getProperty("splice.spark.streaming.blockInterval", "200ms"));
        conf.set("spark.streaming.receiver.maxRate", System.getProperty("splice.spark.streaming.receiver.maxRate", ""));
        conf.set("spark.streaming.receiver.writeAheadLog.enable", System.getProperty("splice.spark.streaming.receiver.writeAheadLog.enable", "false"));
        conf.set("spark.streaming.unpersist", System.getProperty("splice.spark.streaming.unpersist", "true"));
        conf.set("spark.streaming.kafka.maxRatePerPartition", System.getProperty("splice.spark.streaming.kafka.maxRatePerPartition", ""));
        conf.set("spark.streaming.kafka.maxRetries", System.getProperty("splice.spark.streaming.kafka.maxRetries", "1"));
        conf.set("spark.streaming.ui.retainedBatches", System.getProperty("splice.spark.streaming.ui.retainedBatches", "1000"));

        if (LOG.isDebugEnabled()) {
            printConfigProps(conf);
        }

        return new JavaSparkContext(conf);
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
