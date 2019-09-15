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

package com.splicemachine.derby.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.splicemachine.client.SpliceClient;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.derby.hbase.AdapterPipelineEnvironment;
import com.splicemachine.pipeline.PipelineEnvironment;
import com.splicemachine.si.data.hbase.coprocessor.AdapterSIEnvironment;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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

    private SpliceSpark() {} // private constructor forbids creating instances

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
        String threadName = Thread.currentThread().getName();
        if (!SpliceClient.isClient() && !threadName.startsWith("olap-worker-")) {
             // Not running on the Olap Server... raise exception. Use getSessionUnsafe() if you know what you are doing.
            throw new RuntimeException("Trying to get a SparkSession from outside the OlapServer");
        }
        return getSessionUnsafe();
    }

    /** This method is unsafe, it should only be used on tests are as a convenience when trying to
     * get a local Spark Context, it should never be used when implementing Splice operations or functions
     */
    public static synchronized SparkSession getSessionUnsafe() {
        if (!initialized) {
            session = initializeSparkSession();
            ctx =  new JavaSparkContext(session.sparkContext());
            initialized = true;
        } else if (session.sparkContext().isStopped() && !SpliceClient.isClient()) {
            // Try to reinitialize the SparkContext, unless this is SpliceClient, in that case let the user handle the SparkContext
            LOG.warn("SparkContext is stopped, reinitializing...");
            try {
                if (UserGroupInformation.isSecurityEnabled() && UserGroupInformation.isLoginKeytabBased()) {
                    UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.exit(0);
            session = initializeSparkSession();
            ctx =  new JavaSparkContext(session.sparkContext());
        }
        return session;
    }

    public static synchronized JavaSparkContext getContext() {
        SparkSession s = getSession();
        return ctx;
    }

    /** This method is unsafe, it should only be used on tests are as a convenience when trying to
     * get a local Spark Context, it should never be used when implementing Splice operations or functions
     */
    public static synchronized JavaSparkContext getContextUnsafe() {
        SparkSession s = getSessionUnsafe();
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

                boolean tokenEnabled = HConfiguration.getConfiguration().getAuthenticationTokenEnabled();
                boolean debugConnections = HConfiguration.getConfiguration().getAuthenticationTokenDebugConnections();
                int maxConnections = HConfiguration.getConfiguration().getAuthenticationTokenMaxConnections();

                //boot SI components
                SIEnvironment env = SpliceClient.isClient() && tokenEnabled ?
                        AdapterSIEnvironment.loadEnvironment(new SystemClock(),ZkUtils.getRecoverableZooKeeper(),SpliceClient.getConnectionPool(debugConnections,maxConnections)) :
                        HBaseSIEnvironment.loadEnvironment(new SystemClock(),ZkUtils.getRecoverableZooKeeper());
                
                SIDriver driver = env.getSIDriver();

                //make sure the configuration is correct
                SConfiguration config=driver.getConfiguration();

                LOG.info("Splice Client in SpliceSpark "+SpliceClient.isClient());
                
                //boot derby components
                new EngineLifecycleService(new DistributedDerbyStartup(){
                    @Override public void distributedStart() throws IOException{ }
                    @Override public void markBootFinished() throws IOException{ }
                    @Override public boolean connectAsFirstTime(){ return false; }
                },config,false).start();

                EngineDriver engineDriver = EngineDriver.driver();
                assert engineDriver!=null: "Not booted yet!";
                LOG.info("SpliceMachine booted");

                // Create a static statement context to enable nested connections
                EmbedConnection internalConnection = (EmbedConnection)engineDriver.getInternalConnection();
                StatementContext statementContext = internalConnection.getLanguageConnection()
                        .pushStatementContext(true, true, "", null, false, 0);
                statementContext.setSQLAllowed(RoutineAliasInfo.MODIFIES_SQL_DATA, false);

                //boot the pipeline components
                final Clock clock = driver.getClock();
                ContextFactoryDriver cfDriver =ContextFactoryDriverService.loadDriver();
                //we specify rsServices = null here because we don't actually use the receiving side of the Pipeline environment
                PipelineEnvironment pipelineEnv= SpliceClient.isClient() && tokenEnabled ?
                        AdapterPipelineEnvironment.loadEnvironment(clock,cfDriver,SpliceClient.getConnectionPool(debugConnections, maxConnections)) :
                        HBasePipelineEnvironment.loadEnvironment(clock,cfDriver);
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
        conf.set("spark.sql.warehouse.dir", "/user/splice/spark-warehouse");
        conf.set("spark.sql.autoBroadcastJoinThreshold", "-1");

        String schedulerAllocationFile = System.getProperty("splice.spark.scheduler.allocation.file");
        if (schedulerAllocationFile != null) {
            conf.set("spark.scheduler.allocation.file", schedulerAllocationFile);
        }
        conf.set("executor.source.splice-machine.class", "com.splicemachine.derby.stream.spark.SpliceMachineSource");
        conf.set("driver.source.splice-machine.class", "com.splicemachine.derby.stream.spark.SpliceMachineSource");

        // pull out Kerberos and set Yarn properties
        try {
            String principal = null;
            String p = null;
            String configKey = HConfiguration.getConfiguration().getOlapServerExternal() ?
                    "splice.olapServer.hostname" : "hbase.master.hostname";
            String hostname = HConfiguration.unwrapDelegate().get(configKey);
            if (hostname == null) {
                hostname = InetAddress.getLocalHost().getHostName();
                SpliceLogUtils.warn(LOG, "Trying to get local hostname. This could be problem for host with multiple interfaces.");
                SpliceLogUtils.warn(LOG, "For machine with multiple interfaces, please set " + configKey);
            }
            if ((HConfiguration.unwrapDelegate().get("hbase.master.kerberos.principal") != null) ||
                    (HConfiguration.unwrapDelegate().get("hbase.regionserver.kerberos.principal") != null)) {
                if (HConfiguration.unwrapDelegate().get("hbase.master.kerberos.principal") != null) {
                    p = HConfiguration.unwrapDelegate().get("hbase.master.kerberos.principal");
                } else if (HConfiguration.unwrapDelegate().get("hbase.regionserver.kerberos.principal") != null) {
                    p = HConfiguration.unwrapDelegate().get("hbase.regionserver.kerberos.principal");
                }
                principal = SecurityUtil.getServerPrincipal(p, hostname);
                conf.set("spark.yarn.principal", principal);
                SpliceLogUtils.info(LOG, "principal = %s", principal);
            }
        } catch (UnknownHostException e) {
            SpliceLogUtils.warn(LOG, "Could not resolve local host name : %s", e.getMessage());
        } catch (IOException e) {
            SpliceLogUtils.warn(LOG, "Could not replace _HOST: %s", e.getMessage());
        }


        if ((HConfiguration.unwrapDelegate().get("hbase.master.keytab.file") != null) ||
                (HConfiguration.unwrapDelegate().get("hbase.regionserver.keytab.file") != null)) {
            if (HConfiguration.unwrapDelegate().get("hbase.master.keytab.file") != null) {
                conf.set("spark.yarn.keytab", HConfiguration.unwrapDelegate().get("hbase.master.keytab.file"));
            } else if (HConfiguration.unwrapDelegate().get("hbase.regionserver.keytab.file") != null) {
                conf.set("spark.yarn.keytab", HConfiguration.unwrapDelegate().get("hbase.regionserver.keytab.file"));
            }
        }
        String user = System.getProperty("splice.spark.yarn.principal");
        String keytab = System.getProperty("splice.spark.yarn.keytab");

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
        conf.set("spark.sql.avro.compression.codec","uncompressed");
        conf.set("spark.sql.broadcastTimeout", System.getProperty("splice.spark.sql.broadcastTimeout", Integer.toString(Integer.MAX_VALUE)));

         /*
            Application Properties
         */
        conf.set("spark.app.name", System.getProperty("splice.spark.app.name", "SpliceMachine"));
        conf.set("spark.driver.maxResultSize", System.getProperty("splice.spark.driver.maxResultSize", "1g"));
        conf.set("spark.driver.memory", System.getProperty("splice.spark.driver.memory", "1g"));
        conf.set("spark.executor.memory", System.getProperty("splice.spark.executor.memory", "2000M"));
        conf.set("spark.extraListeners", System.getProperty("splice.spark.extraListeners", ""));
        conf.set("spark.local.dir", System.getProperty("splice.spark.local.dir", System.getProperty("java.io.tmpdir")));
        conf.set("spark.logConf", System.getProperty("splice.spark.logConf", "true"));
        conf.set("spark.sql.orc.filterPushdown", System.getProperty("spark.sql.orc.filterPushdown", "true"));
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

        /*

           Spark SQL

         */
        conf.set("spark.sql.retainGroupColumns", "true");

        // Uncomment to disable WholeStageCodeGen for debugging.
        // conf.set("spark.sql.codegen.wholeStage", "false");

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
        SpliceSpark.getContext().setLocalProperty(SCOPE_KEY, null);
        SpliceSpark.getContext().setLocalProperty(SCOPE_OVERRIDE, null);
    }
    
    public synchronized static void setContext(JavaSparkContext sparkContext) {
        session = SparkSession.builder().config(sparkContext.getConf()).getOrCreate(); // Claims this is a singleton from documentation
        ctx = sparkContext;
        initialized = true;
    }

    public synchronized static void setContext(SparkContext sparkContext) {
        setContext(new JavaSparkContext(sparkContext));
    }
}
