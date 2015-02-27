package com.splicemachine.derby.impl.spark;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Splitter;

public class SpliceSpark {
    private static Logger LOG = Logger.getLogger(SpliceSpark.class);
    static JavaSparkContext ctx;
    static boolean initialized = false;
    static boolean active = Boolean.parseBoolean(System.getProperty("splice.spark.enabled", "false"));;
    static volatile JavaSparkContext localContext = null;
    static boolean spliceStaticComponentsSetup = false;

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

    public static synchronized boolean sparkActive() {
        return active;
    }

    public static synchronized void setupSpliceStaticComponents() throws IOException {
        try {
            if (!spliceStaticComponentsSetup) {
                new EmbeddedDriver();
                new SpliceAccessManager();
                SpliceDriver driver = SpliceDriver.driver();
                if (!driver.isStarted()) {
                    driver.start(null); // TODO might cause NPEs....
                }
                if (driver.getUUIDGenerator() == null) {
                    driver.loadUUIDGenerator();
                }
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

    private static JavaSparkContext initializeSparkContext() {
        System.setProperty("spark.driver.port", "0");
        String master = System.getProperty("splice.spark.master", "local[8]");
        String home = System.getProperty("splice.spark.home", null);
        String jars = System.getProperty("splice.spark.jars", "");
        String environment = System.getProperty("splice.spark.env", "");
        String cores = System.getProperty("splice.spark.cores", "8");
        String memory = System.getProperty("splice.spark.memory", "8g");
        String failures = System.getProperty("splice.spark.failures", "4");
        String temp = System.getProperty("splice.spark.tmp", "/tmp");
        String extraOpts = System.getProperty("splice.spark.extra", "");
        LOG.warn("Initializing Spark with:\n master " + master + "\n home " + home + "\n jars " + jars + "\n environment " + environment);
        Map<String, String> properties = Splitter.on(';').omitEmptyStrings().withKeyValueSeparator(Splitter.on('=')).split(environment);
        String [] files = getJarFiles(jars);
        SparkConf conf = new SparkConf();
        conf.setAppName("SpliceMachine");
        conf.setMaster(master);
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "com.splicemachine.derby.impl.spark.SpliceSparkKryoRegistrator");
//        conf.set("spark.serializer", SparkCustomSerializer.class.getName());
        conf.set("spark.executor.memory", "8G");
//				conf.set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.io.compression.codec", "lz4"); // TODO implement custom codec using our Snappy version
        conf.set("spark.kryoserializer.buffer.mb", "8");
        conf.set("spark.kryoserializer.buffer.max.mb", "128");
        conf.set("spark.executor.extraJavaOptions", extraOpts);
        conf.set("spark.kryo.referenceTracking", "false");
        conf.set("spark.storage.memoryFraction", "0.1"); // no caching at the moment
        conf.set("spark.shuffle.memoryFraction", "0.7");
        conf.set("spark.locality.wait", "60000"); // wait up to 60 seconds for a local execution
        conf.set("spark.shuffle.consolidateFiles", "true"); //
//        conf.set("spark.kryo.registrationRequired", "true");
        if (master.startsWith("local[8]")) {
            conf.set("spark.cores.max", "8");
            if (localContext == null) {
                localContext = new JavaSparkContext(conf);
            }
            return localContext;
        } else {
//            conf.setSparkHome(home);
//            conf.setJars(files);
            conf.set("spark.cores.max", cores);
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
}