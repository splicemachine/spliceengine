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
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import com.google.common.base.Splitter;
import org.apache.spark.rdd.RDDOperationScope;

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

    public static synchronized void setupSpliceStaticComponents() throws IOException {
        try {
            if (!spliceStaticComponentsSetup) {
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
        System.setProperty("spark.driver.port", "0");
        String master = System.getProperty("splice.spark.master", "local[8]");
        String home = System.getProperty("splice.spark.home", null);
        String jars = System.getProperty("splice.spark.jars", "");
        String environment = System.getProperty("splice.spark.env", "");
        String cores = System.getProperty("splice.spark.cores", "8");
        String memory = System.getProperty("splice.spark.memory", "2g");
        String failures = System.getProperty("splice.spark.failures", "4");
        String temp = System.getProperty("splice.spark.tmp", "/tmp");
        String extraOpts = System.getProperty("splice.spark.extra", "");
        String extraLibraryPath = System.getProperty("splice.spark.extraLibraryPath", "");
        String extraClassPath = System.getProperty("splice.spark.extraClassPath", "");
        String shuffleMemory = System.getProperty("splice.spark.shuffleMemory", "0.5");
        String schedulerFile = System.getProperty("splice.spark.scheduler.allocation.file");
        String historyServer = System.getProperty("splice.spark.yarn.historyServer.address");


        LOG.warn("Initializing Spark with:\n master " + master + "\n home " + home + "\n jars " + jars + "\n environment " + environment);
        Map<String, String> properties = Splitter.on(';').omitEmptyStrings().withKeyValueSeparator(Splitter.on('=')).split(environment);
        String[] files = getJarFiles(jars);

        SparkConf conf = new SparkConf();
        conf.setAppName("SpliceMachine");
        conf.setMaster(master);
        if (historyServer!=null)
            conf.set("spark.yarn.historyServer.address",historyServer);
//        conf.setJars(files);
        conf.set("spark.yarn.am.waitTime","10");

        if (schedulerFile !=null)
            conf.set("spark.scheduler.allocation.file",schedulerFile);
        conf.set("spark.scheduler.mode", "FAIR");
        conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("executor.source.splice-machine.class","com.splicemachine.derby.stream.spark.SpliceMachineSource");
        conf.set("driver.source.splice-machine.class","com.splicemachine.derby.stream.spark.SpliceMachineSource");
        conf.set("spark.kryo.registrator", "com.splicemachine.derby.impl.SpliceSparkKryoRegistrator");
        // conf.set("spark.serializer", SparkCustomSerializer.class.getName());
        conf.set("spark.executor.memory", "8G");
       // conf.set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.io.compression.codec",HConfiguration.INSTANCE.unwrapDelegate().get("spark.io.compression.codec","lz4"));
        conf.set("spark.io.compression.lz4.block.size","3276800");
        conf.set("spark.shuffle.compress","false");
        conf.set("spark.kryoserializer.buffer.mb", "4");
        conf.set("spark.kryoserializer.buffer.max.mb", "512");
        conf.set("spark.executor.extraJavaOptions", extraOpts);
        conf.set("spark.shuffle.file.buffer.kb","128");
        conf.set("spark.executor.extraLibraryPath", extraLibraryPath);
        conf.set("spark.executor.extraClassPath", extraClassPath);
        conf.set("spark.kryo.referenceTracking", "false");
        conf.set("spark.storage.memoryFraction", "0.1"); // no caching at the moment
        conf.set("spark.shuffle.memoryFraction", shuffleMemory);
        conf.set("spark.locality.wait", "600000"); // wait up to 10 minutes for a local execution
        conf.set("spark.logConf", "true");
        conf.set("spark.executor.cores",cores);
        conf.set("spark.dynamicAllocation.enabled","true");
        conf.set("spark.shuffle.service.enabled","true");


        if (master.startsWith("local[8]")) {
            conf.set("spark.cores.max", "8");
            if (localContext == null) {
                localContext = new JavaSparkContext(conf);
            }
            return localContext;
        } else {
            if (home != null) {
                conf.setSparkHome(home);
            }
            // conf.setJars(files);
            conf.set("spark.executor.memory", memory);
            conf.set("spark.task.maxFailures", failures);
            conf.set("spark.local.dir", temp);
            StringBuilder env = new StringBuilder();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                env.append("-D").append(entry.getKey()).append('=').append(entry.getValue()).append(' ');
            }
            conf.setExecutorEnv("SPARK_JAVA_OPTS", env.toString());
            return new JavaSparkContext(conf);
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
