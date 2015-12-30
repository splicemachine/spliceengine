package com.splicemachine.derby.impl.spark;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDDOperationScope;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.jdbc.EmbeddedDriver;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stream.spark.SpliceMachineSource;
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;

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
                SpliceDriver driver = SpliceDriver.driver();
                if (!driver.isStarted()) {
                    driver.start(null); // TODO might cause NPEs....
                }
                if (driver.getUUIDGenerator() == null) {
                    driver.loadUUIDGenerator(1); // Need to get Spark Port? TODO JL
                }
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
        /*
            Application Properties
         */
        String sparkHome = System.getProperty("splice.spark.home", null);
        String appName = System.getProperty("splice.spark.app.name", "SpliceMachine");
        String driverCores = System.getProperty("splice.spark.driver.cores", "8");
        String maxResultSize = System.getProperty("splice.spark.driver.maxResultSize", "1g");
        String driverMemory = System.getProperty("splice.spark.driver.memory", "1g");
        String executorMemory = System.getProperty("splice.spark.executor.memory", "2g");
        String extraListeners = System.getProperty("splice.spark.extraListeners", "");
        String localDir = System.getProperty("splice.spark.local.dir", System.getProperty("java.io.tmpdir"));
        String logConf = System.getProperty("splice.spark.logConf", "true");
        String master = System.getProperty("splice.spark.master", "yarn-client");

        /*
            Runtime Environment
         */
        String driverExtraClassPath = System.getProperty("splice.spark.driver.extraClassPath");
        String driverExtraJavaOptions = System.getProperty("splice.spark.driver.extraJavaOptions");
        String driverExtraLibraryPath = System.getProperty("splice.spark.driver.extraLibraryPath");
        String driverUserClassPathFirst = System.getProperty("splice.spark.driver.userClassPathFirst");
        String executorExtraClassPath = System.getProperty("splice.spark.executor.extraClassPath");
        String executorExtraJavaOptions = System.getProperty("splice.spark.executor.extraJavaOptions");
        String executorExtraLibraryPath = System.getProperty("splice.spark.executor.extraLibraryPath");
        String executorLogsRollingMaxRetainedFiles = System.getProperty("splice.spark.executor.logs.rolling.maxRetainedFiles");
        String executorLogsRollingMaxSize = System.getProperty("splice.spark.executor.logs.rolling.maxSize");
        String executorLogsRollingStrategy = System.getProperty("splice.spark.executor.logs.rolling.strategy");
        String executorLogsRollingTimeInterval = System.getProperty("splice.spark.executor.logs.rolling.time.interval");
        String executorUserClassPathFirst = System.getProperty("splice.spark.executor.userClassPathFirst");
        // String executorEnvVar = System.getProperty("splice.spark.executorEnv.var"); // this doesnt make sense to have here really, mostly for completeness of if it is seen fit to hard code something here

        /*
            Shuffle Behavior
         */
        String reducerMaxSizeInFlight = System.getProperty("splice.spark.reducer.maxSizeInFlight");
        String shuffleBlockTransferService = System.getProperty("splice.spark.shuffle.blockTransferService");
        String shuffleCompress = System.getProperty("splice.spark.shuffle.compress");
        String shuffleConsolidateFiles = System.getProperty("splice.spark.shuffle.consolidateFiles");
        String shuffleFileBuffer = System.getProperty("splice.spark.shuffle.file.buffer");
        String shuffleIoMaxRetries = System.getProperty("splice.spark.shuffle.io.maxRetries");
        String shuffleIoNumConnectionsPerPeer = System.getProperty("splice.spark.shuffle.io.numConnectionsPerPeer");
        String shuffleIoPreferDirectBufs = System.getProperty("splice.spark.shuffle.io.preferDirectBufs");
        String shuffleIoRetryWait = System.getProperty("splice.spark.shuffle.io.retryWait");
        String shuffleManager = System.getProperty("splice.spark.shuffle.manager");
        String shuffleMemoryFraction = System.getProperty("splice.spark.shuffle.memoryFraction");
        String shuffleserviceEnabled = System.getProperty("splice.spark.shuffle.service.enabled");
        String shuffleServicePort = System.getProperty("splice.spark.shuffle.service.port");
        String shufflesortBypassMergeThreshold = System.getProperty("splice.spark.shuffle.sort.bypassMergeThreshold");
        String shuffleSpill = System.getProperty("splice.spark.shuffle.spill");
        String shuffleSpillCompress = System.getProperty("splice.spark.shuffle.spill.compress");

        /*
            Spark UI
        */
        String eventLogCompress = System.getProperty("splice.spark.eventLog.compress");
        String eventLogDir = System.getProperty("splice.spark.eventLog.dir");
        String eventLogEnabled = System.getProperty("splice.spark.eventLog.enabled");
        String uiKillEnabled = System.getProperty("splice.spark.ui.killEnabled");
        String uiPort = System.getProperty("splice.spark.ui.port");
        String uiRetainedJobs = System.getProperty("splice.spark.ui.retainedJobs");
        String uiRetainedStages = System.getProperty("splice.spark.ui.retainedStages");
        String workerUiRetainedExecutors = System.getProperty("splice.spark.worker.ui.retainedExecutors");
        String workerUiRetainedDrivers = System.getProperty("splice.spark.worker.ui.retainedDrivers");

        /*
            Compression and Serialization
        */
        String broadcastCompress = System.getProperty("splice.spark.broadcast.compress");
        String closureSerializer = System.getProperty("splice.spark.closure.serializer");
        String ioCompressionCodec = System.getProperty("splice.spark.io.compression.codec");
        String ioCompressionLz4BlockSize = System.getProperty("splice.spark.io.compression.lz4.blockSize");
        String ioCompressionSnappyBlockSize = System.getProperty("splice.spark.io.compression.snappy.blockSize");
        String kryoClassesToRegister = System.getProperty("splice.spark.kryo.classesToRegister");
        String kryoReferenceTracking = System.getProperty("splice.spark.kryo.referenceTracking");
        String kryoRegistrationRequired = System.getProperty("splice.spark.kryo.registrationRequired");
        String kryoRegistrator = System.getProperty("splice.spark.kryo.registrator");
        String kryoserializerBufferMax = System.getProperty("splice.spark.kryoserializer.buffer.max");
        String kryoserializerBuffer = System.getProperty("splice.spark.kryoserializer.buffer");
        String rddCompress = System.getProperty("splice.spark.rdd.compress");
        String serializer = System.getProperty("splice.spark.serializer");
        String serializerObjectStreamReset = System.getProperty("splice.spark.serializer.objectStreamReset");

        /*
            Execution Behavior
        */
        String broadcastBlockSize = System.getProperty("splice.spark.broadcast.blockSize");
        String broadcastFactory = System.getProperty("splice.spark.broadcast.factory");
        String cleanerTtl = System.getProperty("splice.spark.cleaner.ttl");
        String executorCores = System.getProperty("splice.spark.executor.cores");
        String defaultParallelism = System.getProperty("splice.spark.default.parallelism");
        String executorHeartbeatInterval = System.getProperty("splice.spark.executor.heartbeatInterval");
        String filesFetchTimeout = System.getProperty("splice.spark.files.fetchTimeout");
        String filesUseFetchCache = System.getProperty("splice.spark.files.useFetchCache");
        String filesOverwrite = System.getProperty("splice.spark.files.overwrite");
        String hadoopCloneConf = System.getProperty("splice.spark.hadoop.cloneConf");
        String hadoopValidateOutputSpecs = System.getProperty("splice.spark.hadoop.validateOutputSpecs");
        String storageMemoryFraction = System.getProperty("splice.spark.storage.memoryFraction");
        String storageMemoryMapThreshold = System.getProperty("splice.spark.storage.memoryMapThreshold");
        String storageUnrollFraction = System.getProperty("splice.spark.storage.unrollFraction");
        String externalBlockStoreBlockManager = System.getProperty("splice.spark.externalBlockStore.blockManager");
        String externalBlockStoreBaseDir = System.getProperty("splice.spark.externalBlockStore.baseDir");
        String externalBlockStoreUrl = System.getProperty("splice.spark.externalBlockStore.url");

        /*
            Networking
        */
        String akkaFrameSize = System.getProperty("splice.spark.akka.frameSize");
        String akkaHeartbeatInterval = System.getProperty("splice.spark.akka.heartbeat.interval");
        String akkaHeartbeatPauses = System.getProperty("splice.spark.akka.heartbeat.pauses");
        String akkaThreads = System.getProperty("splice.spark.akka.threads");
        String akkaTimeout = System.getProperty("splice.spark.akka.timeout");
        String blockManagerPort = System.getProperty("splice.spark.blockManager.port");
        String broadcastPort = System.getProperty("splice.spark.broadcast.port");
        String driverHost = System.getProperty("splice.spark.driver.host");
        String driverPort = System.getProperty("splice.spark.driver.port");
        String executorPort = System.getProperty("splice.spark.executor.port");
        String fileserverPort = System.getProperty("splice.spark.fileserver.port");
        String networkTimeout = System.getProperty("splice.spark.network.timeout");
        String portMaxRetries = System.getProperty("splice.spark.port.maxRetries");
        String replClassServerPort = System.getProperty("splice.spark.replClassServer.port");
        String rpcNumRetries = System.getProperty("splice.spark.rpc.numRetries");
        String rpcRetryWait = System.getProperty("splice.spark.rpc.retry.wait");
        String rpcAskTimeout = System.getProperty("splice.spark.rpc.askTimeout");
        String rpcLookupTimeout = System.getProperty("splice.spark.rpc.lookupTimeout");

        /*
            Scheduling
        */
        String coresMax = System.getProperty("splice.spark.cores.max");
        String localityWait = System.getProperty("splice.spark.locality.wait");
        String localityWaitNode = System.getProperty("splice.spark.locality.wait.node");
        String localityWaitProcess = System.getProperty("splice.spark.locality.wait.process");
        String localityWaitRack = System.getProperty("splice.spark.locality.wait.rack");
        String schedulerMaxRegisteredResourcesWaitingTime = System.getProperty("splice.spark.scheduler.maxRegisteredResourcesWaitingTime");
        String schedulerMinRegisteredResourcesRatio = System.getProperty("splice.spark.scheduler.minRegisteredResourcesRatio");
        String schedulerMode = System.getProperty("splice.spark.scheduler.mode");
        String schedulerReviveInterval = System.getProperty("splice.spark.scheduler.revive.interval");
        String speculation = System.getProperty("splice.spark.speculation");
        String speculationInterval = System.getProperty("splice.spark.speculation.interval");
        String speculationMultiplier = System.getProperty("splice.spark.speculation.multiplier");
        String speculationQuantile = System.getProperty("splice.spark.speculation.quantile");
        String taskCpus = System.getProperty("splice.spark.task.cpus");
        String taskMaxFailures = System.getProperty("splice.spark.task.maxFailures");

        /*
            Dynamic Allocation
        */
        String dynamicAllocationEnabled = System.getProperty("splice.spark.dynamicAllocation.enabled");
        String dynamicAllocationExecutorIdleTimeout = System.getProperty("splice.spark.dynamicAllocation.executorIdleTimeout");
        String dynamicAllocationCachedExecutorIdleTimeout = System.getProperty("splice.spark.dynamicAllocation.cachedExecutorIdleTimeout");
        String dynamicAllocationInitialExecutors = System.getProperty("splice.spark.dynamicAllocation.initialExecutors");
        String dynamicAllocationMaxExecutors = System.getProperty("splice.spark.dynamicAllocation.maxExecutors");
        String dynamicAllocationMinExecutors = System.getProperty("splice.spark.dynamicAllocation.minExecutors");
        String dynamicAllocationSchedulerBacklogTimeout = System.getProperty("splice.spark.dynamicAllocation.schedulerBacklogTimeout");
        String dynamicAllocationSustainedSchedulerBacklogTimeout = System.getProperty("splice.spark.dynamicAllocation.sustainedSchedulerBacklogTimeout");

        /*
            Security
        */
        String aclsEnable = System.getProperty("splice.spark.acls.enable");
        String adminAcls = System.getProperty("splice.spark.admin.acls");
        String authenticate = System.getProperty("splice.spark.authenticate");
        String authenticateSecret = System.getProperty("splice.spark.authenticate.secret");
        String coreConnectionAckWaitTimeout = System.getProperty("splice.spark.core.connection.ack.wait.timeout");
        String coreConnectionAuthWaitTimeout = System.getProperty("splice.spark.core.connection.auth.wait.timeout");
        String modifyAcls = System.getProperty("splice.spark.modify.acls");
        String uiFilters = System.getProperty("splice.spark.ui.filters");
        String comFilterClassParams = System.getProperty("splice.spark.com.filter.class.params");
        String uiViewAcls = System.getProperty("splice.spark.ui.view.acls");

        /*
            Encryption
        */
        String sslEnabled = System.getProperty("splice.spark.ssl.enabled");
        String sslEnabledAlgorithms = System.getProperty("splice.spark.ssl.enabledAlgorithms");
        String sslKeyPassword = System.getProperty("splice.spark.ssl.keyPassword");
        String sslKeyStore = System.getProperty("splice.spark.ssl.keyStore");
        String sslKeyStorePassword = System.getProperty("splice.spark.ssl.keyStorePassword");
        String sslProtocol = System.getProperty("splice.spark.ssl.protocol");
        String sslTrustStore = System.getProperty("splice.spark.ssl.trustStore");
        String sslTrustStorePassword = System.getProperty("splice.spark.ssl.trustStorePassword");

        /*
            Spark Streaming
        */
        String streamingBackpressureEnabled = System.getProperty("splice.spark.streaming.backpressure.enabled", "false");
        String streamingBlockInterval = System.getProperty("splice.spark.streaming.blockInterval", "200ms");
        String streamingReceiverMaxRate = System.getProperty("splice.spark.streaming.receiver.maxRate", "");
        String streamingReceiverWriteAheadLogEnable = System.getProperty("splice.spark.streaming.receiver.writeAheadLog.enable", "false");
        String streamingUnpersist = System.getProperty("splice.spark.streaming.unpersist", "true");
        String streamingKafkaMaxRatePerPartition = System.getProperty("splice.spark.streaming.kafka.maxRatePerPartition", "");
        String streamingKafkaMaxRetries = System.getProperty("splice.spark.streaming.kafka.maxRetries", "1");
        String streamingUiRetainedBatches = System.getProperty("splice.spark.streaming.ui.retainedBatches", "1000");

        /*
            Spark (on YARN) Properties
        */
        String yarnAmMemory = System.getProperty("splice.spark.yarn.am.memory");
        String yarnAmCores = System.getProperty("splice.spark.yarn.am.cores");
        String yarnAmWaitTime = System.getProperty("splice.spark.yarn.am.waitTime");
        String yarnSubmitFileReplication = System.getProperty("splice.spark.yarn.submit.file.replication");
        String yarnPreserveStagingFiles = System.getProperty("splice.spark.yarn.preserve.staging.files");
        String yarnSchedulerHeartbeatIntervalMs = System.getProperty("splice.spark.yarn.scheduler.heartbeat.interval-ms");
        String yarnSchedulerInitialAllocationInterval = System.getProperty("splice.spark.yarn.scheduler.initial-allocation.interval");
        String yarnMaxExecutorFailures = System.getProperty("splice.spark.yarn.max.executor.failures");
        String yarnHistoryServerAddress = System.getProperty("splice.spark.yarn.historyServer.address");
        String yarnDistArchives = System.getProperty("splice.spark.yarn.dist.archives");
        String yarnDistFiles = System.getProperty("splice.spark.yarn.dist.files");
        String executorInstances = System.getProperty("splice.spark.executor.instances");
        String yarnExecutorMemoryOverhead = System.getProperty("splice.spark.yarn.executor.memoryOverhead");
        String yarnDriverMemoryOverhead = System.getProperty("splice.spark.yarn.driver.memoryOverhead");
        String yarnAmMemoryOverhead = System.getProperty("splice.spark.yarn.am.memoryOverhead");
        String yarnAmPort = System.getProperty("splice.spark.yarn.am.port");
        String yarnQueue = System.getProperty("splice.spark.yarn.queue");
        String yarnJar = System.getProperty("splice.spark.yarn.jar");
        String yarnAccessNamenodes = System.getProperty("splice.spark.yarn.access.namenodes");
        // String yarnAppMasterEnv.[EnvironmentVariableName] = System.getProperty("splice.spark.yarn.appMasterEnv.[EnvironmentVariableName]"); // this doesnt make sense to have here really, mostly for completeness of if it is seen fit to hard code something here
        String yarnContainerLauncherMaxThreads = System.getProperty("splice.spark.yarn.containerLauncherMaxThreads");
        String yarnAmExtraJavaOptions = System.getProperty("splice.spark.yarn.am.extraJavaOptions");
        String yarnAmExtraLibraryPath = System.getProperty("splice.spark.yarn.am.extraLibraryPath");
        String yarnMaxAppAttempts = System.getProperty("splice.spark.yarn.maxAppAttempts");
        String yarnSubmitWaitAppCompletion = System.getProperty("splice.spark.yarn.submit.waitAppCompletion");
        String yarnExecutorNodeLabelExpression = System.getProperty("splice.spark.yarn.executor.nodeLabelExpression");
        String yarnKeytab = System.getProperty("splice.spark.yarn.keytab");
        String yarnPrincipal = System.getProperty("splice.spark.yarn.principal");
        String yarnConfigGatewayPath = System.getProperty("splice.spark.yarn.config.gatewayPath");
        String yarnConfigReplacementPath = System.getProperty("splice.spark.yarn.config.replacementPath");


        String schedulerAllocationFile = System.getProperty("splice.spark.scheduler.allocation.file");


        LOG.warn("##############################################################################");
        LOG.warn("    Initializing Spark with: master = " + master);
        LOG.warn("##############################################################################");

        SparkConf conf = new SparkConf();


        if (schedulerAllocationFile != null) {
            conf.set("spark.scheduler.allocation.file", schedulerAllocationFile);
        }

        if (master.startsWith("local[8]")) {
            conf.set("spark.cores.max", "8");
            if (localContext == null) {
                localContext = new JavaSparkContext(conf);
            }
            return localContext;
        } else {
            if (sparkHome != null) {
                conf.setSparkHome(sparkHome);
            }
            }
        conf.set("executor.source.splice-machine.class", "com.splicemachine.derby.stream.spark.SpliceMachineSource");
        conf.set("driver.source.splice-machine.class", "com.splicemachine.derby.stream.spark.SpliceMachineSource");

        /*
        Application Properties
        */
        conf.set("spark.app.name", appName);
        conf.set("spark.driver.cores", driverCores);
        conf.set("spark.driver.maxResultSize", maxResultSize);
        conf.set("spark.driver.memory", driverMemory);
        conf.set("spark.executor.memory", executorMemory);
        conf.set("spark.extraListeners", extraListeners);
        conf.set("spark.local.dir", localDir);
        conf.set("spark.logConf", logConf);
        conf.set("spark.master", master);

        /*
            Runtime Environment
        */
        if (driverExtraClassPath != null) conf.set("spark.driver.extraClassPath", driverExtraClassPath);
        if (driverExtraJavaOptions != null) conf.set("spark.driver.extraJavaOptions", driverExtraJavaOptions);
        if (driverExtraLibraryPath != null) conf.set("spark.driver.extraLibraryPath", driverExtraLibraryPath);
        if (driverUserClassPathFirst != null) conf.set("spark.driver.userClassPathFirst", driverUserClassPathFirst);
        if (executorExtraClassPath != null) conf.set("spark.executor.extraClassPath", executorExtraClassPath);
        if (executorExtraJavaOptions != null) conf.set("spark.executor.extraJavaOptions", executorExtraJavaOptions);
        if (executorExtraLibraryPath != null) conf.set("spark.executor.extraLibraryPath", executorExtraLibraryPath);
        if (executorLogsRollingMaxRetainedFiles != null) conf.set("spark.executor.logs.rolling.maxRetainedFiles", executorLogsRollingMaxRetainedFiles);
        if (executorLogsRollingMaxSize != null) conf.set("spark.executor.logs.rolling.maxSize", executorLogsRollingMaxSize);
        if (executorLogsRollingStrategy != null) conf.set("spark.executor.logs.rolling.strategy", executorLogsRollingStrategy);
        if (executorLogsRollingTimeInterval != null) conf.set("spark.executor.logs.rolling.time.interval", executorLogsRollingTimeInterval);
        if (executorUserClassPathFirst != null) conf.set("spark.executor.userClassPathFirst", executorUserClassPathFirst);

        /*
            Shuffle Behavior
        */
        if (reducerMaxSizeInFlight != null) conf.set("spark.reducer.maxSizeInFlight", reducerMaxSizeInFlight);
        if (shuffleBlockTransferService != null) conf.set("spark.shuffle.blockTransferService", shuffleBlockTransferService);
        if (shuffleCompress != null) conf.set("spark.shuffle.compress", shuffleCompress);
        if (shuffleConsolidateFiles != null) conf.set("spark.shuffle.consolidateFiles", shuffleConsolidateFiles);
        if (shuffleFileBuffer != null) conf.set("spark.shuffle.file.buffer", shuffleFileBuffer);
        if (shuffleIoMaxRetries != null) conf.set("spark.shuffle.io.maxRetries", shuffleIoMaxRetries);
        if (shuffleIoNumConnectionsPerPeer != null) conf.set("spark.shuffle.io.numConnectionsPerPeer", shuffleIoNumConnectionsPerPeer);
        if (shuffleIoPreferDirectBufs != null) conf.set("spark.shuffle.io.preferDirectBufs", shuffleIoPreferDirectBufs);
        if (shuffleIoRetryWait != null) conf.set("spark.shuffle.io.retryWait", shuffleIoRetryWait);
        if (shuffleManager != null) conf.set("spark.shuffle.manager", shuffleManager);
        if (shuffleMemoryFraction != null) conf.set("spark.shuffle.memoryFraction", shuffleMemoryFraction);
        if (shuffleserviceEnabled != null) conf.set("spark.shuffle.service.enabled", shuffleserviceEnabled);
        if (shuffleServicePort != null) conf.set("spark.shuffle.service.port", shuffleServicePort);
        if (shufflesortBypassMergeThreshold != null) conf.set("spark.shuffle.sort.bypassMergeThreshold", shufflesortBypassMergeThreshold);
        if (shuffleSpill != null) conf.set("spark.shuffle.spill", shuffleSpill);
        if (shuffleSpillCompress != null) conf.set("spark.shuffle.spill.compress", shuffleSpillCompress);

        /*
            Spark UI
        */
        if (eventLogCompress != null) conf.set("spark.eventLog.compress", eventLogCompress);
        if (eventLogDir != null) conf.set("spark.eventLog.dir", eventLogDir);
        if (eventLogEnabled != null) conf.set("spark.eventLog.enabled", eventLogEnabled);
        if (uiKillEnabled != null) conf.set("spark.ui.killEnabled", uiKillEnabled);
        if (uiPort != null) conf.set("spark.ui.port", uiPort);
        if (uiRetainedJobs != null) conf.set("spark.ui.retainedJobs", uiRetainedJobs);
        if (uiRetainedStages != null) conf.set("spark.ui.retainedStages", uiRetainedStages);
        if (workerUiRetainedExecutors != null) conf.set("spark.worker.ui.retainedExecutors", workerUiRetainedExecutors);
        if (workerUiRetainedDrivers != null) conf.set("spark.worker.ui.retainedDrivers", workerUiRetainedDrivers);

        /*
            Compression and Serialization
        */
        if (broadcastCompress != null) conf.set("spark.broadcast.compress", broadcastCompress);
        if (closureSerializer != null) conf.set("spark.closure.serializer", closureSerializer);
        // if (ioCompressionCodec != null) conf.set("spark.io.compression.codec", ioCompressionCodec);
        // TODO can this be set/overridden fwith system property, why do we use SpliceConstants?
        conf.set("spark.io.compression.codec", SpliceConstants.config.get("spark.io.compression.codec", "lz4"));
        if (ioCompressionLz4BlockSize != null) conf.set("spark.io.compression.lz4.blockSize", ioCompressionLz4BlockSize);
        if (ioCompressionSnappyBlockSize != null) conf.set("spark.io.compression.snappy.blockSize", ioCompressionSnappyBlockSize);
        if (kryoClassesToRegister != null) conf.set("spark.kryo.classesToRegister", kryoClassesToRegister);
        if (kryoReferenceTracking != null) conf.set("spark.kryo.referenceTracking", kryoReferenceTracking);
        if (kryoRegistrationRequired != null) conf.set("spark.kryo.registrationRequired", kryoRegistrationRequired);
        if (kryoRegistrator != null) conf.set("spark.kryo.registrator", kryoRegistrator);
        if (kryoserializerBufferMax != null) conf.set("spark.kryoserializer.buffer.max", kryoserializerBufferMax);
        if (kryoserializerBuffer != null) conf.set("spark.kryoserializer.buffer", kryoserializerBuffer);
        if (rddCompress != null) conf.set("spark.rdd.compress", rddCompress);
        if (serializer != null) conf.set("spark.serializer", serializer);
        if (serializerObjectStreamReset != null) conf.set("spark.serializer.objectStreamReset", serializerObjectStreamReset);

        /*
            Execution Behavior
        */
        if (broadcastBlockSize != null) conf.set("spark.broadcast.blockSize", broadcastBlockSize);
        if (broadcastFactory != null) conf.set("spark.broadcast.factory", broadcastFactory);
        if (cleanerTtl != null) conf.set("spark.cleaner.ttl", cleanerTtl);
        if (executorCores != null) conf.set("spark.executor.cores", executorCores);
        if (defaultParallelism != null) conf.set("spark.default.parallelism", defaultParallelism);
        if (executorHeartbeatInterval != null) conf.set("spark.executor.heartbeatInterval", executorHeartbeatInterval);
        if (filesFetchTimeout != null) conf.set("spark.files.fetchTimeout", filesFetchTimeout);
        if (filesUseFetchCache != null) conf.set("spark.files.useFetchCache", filesUseFetchCache);
        if (filesOverwrite != null) conf.set("spark.files.overwrite", filesOverwrite);
        if (hadoopCloneConf != null) conf.set("spark.hadoop.cloneConf", hadoopCloneConf);
        if (hadoopValidateOutputSpecs != null) conf.set("spark.hadoop.validateOutputSpecs", hadoopValidateOutputSpecs);
        if (storageMemoryFraction != null) conf.set("spark.storage.memoryFraction", storageMemoryFraction);
        if (storageMemoryMapThreshold != null) conf.set("spark.storage.memoryMapThreshold", storageMemoryMapThreshold);
        if (storageUnrollFraction != null) conf.set("spark.storage.unrollFraction", storageUnrollFraction);
        if (externalBlockStoreBlockManager != null) conf.set("spark.externalBlockStore.blockManager", externalBlockStoreBlockManager);
        if (externalBlockStoreBaseDir != null) conf.set("spark.externalBlockStore.baseDir", externalBlockStoreBaseDir);
        if (externalBlockStoreUrl != null) conf.set("spark.externalBlockStore.url", externalBlockStoreUrl);

        /*
            Networking
        */
        if (akkaFrameSize != null) conf.set("spark.akka.frameSize", akkaFrameSize);
        if (akkaHeartbeatInterval != null) conf.set("spark.akka.heartbeat.interval", akkaHeartbeatInterval);
        if (akkaHeartbeatPauses != null) conf.set("spark.akka.heartbeat.pauses", akkaHeartbeatPauses);
        if (akkaThreads != null) conf.set("spark.akka.threads", akkaThreads);
        if (akkaTimeout != null) conf.set("spark.akka.timeout", akkaTimeout);
        if (blockManagerPort != null) conf.set("spark.blockManager.port", blockManagerPort);
        if (broadcastPort != null) conf.set("spark.broadcast.port", broadcastPort);
        if (driverHost != null) conf.set("spark.driver.host", driverHost);
        if (driverPort != null) conf.set("spark.driver.port", driverPort);
        if (executorPort != null) conf.set("spark.executor.port", executorPort);
        if (fileserverPort != null) conf.set("spark.fileserver.port", fileserverPort);
        if (networkTimeout != null) conf.set("spark.network.timeout", networkTimeout);
        if (portMaxRetries != null) conf.set("spark.port.maxRetries", portMaxRetries);
        if (replClassServerPort != null) conf.set("spark.replClassServer.port", replClassServerPort);
        if (rpcNumRetries != null) conf.set("spark.rpc.numRetries", rpcNumRetries);
        if (rpcRetryWait != null) conf.set("spark.rpc.retry.wait", rpcRetryWait);
        if (rpcAskTimeout != null) conf.set("spark.rpc.askTimeout", rpcAskTimeout);
        if (rpcLookupTimeout != null) conf.set("spark.rpc.lookupTimeout", rpcLookupTimeout);

        /*
            Scheduling
        */
        if (coresMax != null) conf.set("spark.cores.max", coresMax);
        if (localityWait != null) conf.set("spark.locality.wait", localityWait);
        if (localityWaitNode != null) conf.set("spark.locality.wait.node", localityWaitNode);
        if (localityWaitProcess != null) conf.set("spark.locality.wait.process", localityWaitProcess);
        if (localityWaitRack != null) conf.set("spark.locality.wait.rack", localityWaitRack);
        if (schedulerMaxRegisteredResourcesWaitingTime != null) conf.set("spark.scheduler.maxRegisteredResourcesWaitingTime", schedulerMaxRegisteredResourcesWaitingTime);
        if (schedulerMinRegisteredResourcesRatio != null) conf.set("spark.scheduler.minRegisteredResourcesRatio", schedulerMinRegisteredResourcesRatio);
        if (schedulerMode != null) conf.set("spark.scheduler.mode", schedulerMode);
        if (schedulerReviveInterval != null) conf.set("spark.scheduler.revive.interval", schedulerReviveInterval);
        if (speculation != null) conf.set("spark.speculation", speculation);
        if (speculationInterval != null) conf.set("spark.speculation.interval", speculationInterval);
        if (speculationMultiplier != null) conf.set("spark.speculation.multiplier", speculationMultiplier);
        if (speculationQuantile != null) conf.set("spark.speculation.quantile", speculationQuantile);
        if (taskCpus != null) conf.set("spark.task.cpus", taskCpus);
        if (taskMaxFailures != null) conf.set("spark.task.maxFailures", taskMaxFailures);
        if (schedulerAllocationFile != null) conf.set("spark.scheduler.allocation.file", schedulerAllocationFile);

        /*
            Dynamic Allocation
        */
        if (dynamicAllocationEnabled != null) conf.set("spark.dynamicAllocation.enabled", dynamicAllocationEnabled);
        if (dynamicAllocationExecutorIdleTimeout != null) conf.set("spark.dynamicAllocation.executorIdleTimeout", dynamicAllocationExecutorIdleTimeout);
        if (dynamicAllocationCachedExecutorIdleTimeout != null) conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", dynamicAllocationCachedExecutorIdleTimeout);
        if (dynamicAllocationInitialExecutors != null) conf.set("spark.dynamicAllocation.initialExecutors", dynamicAllocationInitialExecutors);
        if (dynamicAllocationMaxExecutors != null) conf.set("spark.dynamicAllocation.maxExecutors", dynamicAllocationMaxExecutors);
        if (dynamicAllocationMinExecutors != null) conf.set("spark.dynamicAllocation.minExecutors", dynamicAllocationMinExecutors);
        if (dynamicAllocationSchedulerBacklogTimeout != null) conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", dynamicAllocationSchedulerBacklogTimeout);
        if (dynamicAllocationSustainedSchedulerBacklogTimeout != null) conf.set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", dynamicAllocationSustainedSchedulerBacklogTimeout);

        /*
            Security
        */
        if (aclsEnable != null) conf.set("spark.acls.enable", aclsEnable);
        if (adminAcls != null) conf.set("spark.admin.acls", adminAcls);
        if (authenticate != null) conf.set("spark.authenticate", authenticate);
        if (authenticateSecret != null) conf.set("spark.authenticate.secret", authenticateSecret);
        if (coreConnectionAckWaitTimeout != null) conf.set("spark.core.connection.ack.wait.timeout", coreConnectionAckWaitTimeout);
        if (coreConnectionAuthWaitTimeout != null) conf.set("spark.core.connection.auth.wait.timeout", coreConnectionAuthWaitTimeout);
        if (modifyAcls != null) conf.set("spark.modify.acls", modifyAcls);
        if (uiFilters != null) conf.set("spark.ui.filters", uiFilters);
        if (comFilterClassParams != null) conf.set("spark.com.filter.class.params", comFilterClassParams);
        if (uiViewAcls != null) conf.set("spark.ui.view.acls", uiViewAcls);

        /*
            Encryption
        */
        if (sslEnabled != null) conf.set("spark.ssl.enabled", sslEnabled);
        if (sslEnabledAlgorithms != null) conf.set("spark.ssl.enabledAlgorithms", sslEnabledAlgorithms);
        if (sslKeyPassword != null) conf.set("spark.ssl.keyPassword", sslKeyPassword);
        if (sslKeyStore != null) conf.set("spark.ssl.keyStore", sslKeyStore);
        if (sslKeyStorePassword != null) conf.set("spark.ssl.keyStorePassword", sslKeyStorePassword);
        if (sslProtocol != null) conf.set("spark.ssl.protocol", sslProtocol);
        if (sslTrustStore != null) conf.set("spark.ssl.trustStore", sslTrustStore);
        if (sslTrustStorePassword != null) conf.set("spark.ssl.trustStorePassword", sslTrustStorePassword);

        /*
            Spark Streaming
        */
        if (streamingBackpressureEnabled != null) conf.set("spark.streaming.backpressure.enabled", streamingBackpressureEnabled);
        if (streamingBlockInterval != null) conf.set("spark.streaming.blockInterval", streamingBlockInterval);
        if (streamingReceiverMaxRate != null) conf.set("spark.streaming.receiver.maxRate", streamingReceiverMaxRate);
        if (streamingReceiverWriteAheadLogEnable != null) conf.set("spark.streaming.receiver.writeAheadLog.enable", streamingReceiverWriteAheadLogEnable);
        if (streamingUnpersist != null) conf.set("spark.streaming.unpersist", streamingUnpersist);
        if (streamingKafkaMaxRatePerPartition != null) conf.set("spark.streaming.kafka.maxRatePerPartition", streamingKafkaMaxRatePerPartition);
        if (streamingKafkaMaxRetries != null) conf.set("spark.streaming.kafka.maxRetries", streamingKafkaMaxRetries);
        if (streamingUiRetainedBatches != null) conf.set("spark.streaming.ui.retainedBatches", streamingUiRetainedBatches);

        /*
            Spark (on YARN) Properties
        */
        if (yarnAmMemory != null) conf.set("spark.yarn.am.memory", yarnAmMemory);
        if (yarnAmCores != null) conf.set("spark.yarn.am.cores", yarnAmCores);
        if (yarnAmWaitTime != null) conf.set("spark.yarn.am.waitTime", yarnAmWaitTime);
        if (yarnSubmitFileReplication != null) conf.set("spark.yarn.submit.file.replication", yarnSubmitFileReplication);
        if (yarnPreserveStagingFiles != null) conf.set("spark.yarn.preserve.staging.files", yarnPreserveStagingFiles);
        if (yarnSchedulerHeartbeatIntervalMs != null) conf.set("spark.yarn.scheduler.heartbeat.interval-ms", yarnSchedulerHeartbeatIntervalMs);
        if (yarnSchedulerInitialAllocationInterval != null) conf.set("spark.yarn.scheduler.initial-allocation.interval", yarnSchedulerInitialAllocationInterval);
        if (yarnMaxExecutorFailures != null) conf.set("spark.yarn.max.executor.failures", yarnMaxExecutorFailures);
        if (yarnHistoryServerAddress != null) conf.set("spark.yarn.historyServer.address", yarnHistoryServerAddress);
        if (yarnDistArchives != null) conf.set("spark.yarn.dist.archives", yarnDistArchives);
        if (yarnDistFiles != null) conf.set("spark.yarn.dist.files", yarnDistFiles);
        if (executorInstances != null) conf.set("spark.executor.instances", executorInstances);
        if (yarnExecutorMemoryOverhead != null) conf.set("spark.yarn.executor.memoryOverhead", yarnExecutorMemoryOverhead);
        if (yarnDriverMemoryOverhead != null) conf.set("spark.yarn.driver.memoryOverhead", yarnDriverMemoryOverhead);
        if (yarnAmMemoryOverhead != null) conf.set("spark.yarn.am.memoryOverhead", yarnAmMemoryOverhead);
        if (yarnAmPort != null) conf.set("spark.yarn.am.port", yarnAmPort);
        if (yarnQueue != null) conf.set("spark.yarn.queue", yarnQueue);
        if (yarnJar != null) conf.set("spark.yarn.jar", yarnJar);
        if (yarnAccessNamenodes != null) conf.set("spark.yarn.access.namenodes", yarnAccessNamenodes);
        if (yarnContainerLauncherMaxThreads != null) conf.set("spark.yarn.containerLauncherMaxThreads", yarnContainerLauncherMaxThreads);
        if (yarnAmExtraJavaOptions != null) conf.set("spark.yarn.am.extraJavaOptions", yarnAmExtraJavaOptions);
        if (yarnAmExtraLibraryPath != null) conf.set("spark.yarn.am.extraLibraryPath", yarnAmExtraLibraryPath);
        if (yarnMaxAppAttempts != null) conf.set("spark.yarn.maxAppAttempts", yarnMaxAppAttempts);
        if (yarnSubmitWaitAppCompletion != null) conf.set("spark.yarn.submit.waitAppCompletion", yarnSubmitWaitAppCompletion);
        if (yarnExecutorNodeLabelExpression != null) conf.set("spark.yarn.executor.nodeLabelExpression", yarnExecutorNodeLabelExpression);
        if (yarnKeytab != null) conf.set("spark.yarn.keytab", yarnKeytab);
        if (yarnPrincipal != null) conf.set("spark.yarn.principal", yarnPrincipal);
        if (yarnConfigGatewayPath != null) conf.set("spark.yarn.config.gatewayPath", yarnConfigGatewayPath);
        if (yarnConfigReplacementPath != null) conf.set("spark.yarn.config.replacementPath", yarnConfigReplacementPath);

            return new JavaSparkContext(conf);
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
