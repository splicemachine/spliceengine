package com.splicemachine.derby.hbase;

import com.google.common.io.Closeables;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.ddl.DDLCoordinationFactory;
import com.splicemachine.derby.ddl.DDLWatcher;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.scheduler.*;
import com.splicemachine.derby.impl.sql.execute.sequence.AbstractSequenceKey;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.stats.StatisticsStorage;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.logging.DerbyOutputLoggerWriter;
import com.splicemachine.derby.management.StatementManager;
import com.splicemachine.derby.management.XplainTaskReporter;
import com.splicemachine.derby.utils.ErrorReporter;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.SpliceMetrics;
import com.splicemachine.hbase.backup.BackupHFileCleaner;
import com.splicemachine.job.*;
import com.splicemachine.pipeline.api.Service;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.tools.CachedResourcePool;
import com.splicemachine.tools.EmbedConnectionMaker;
import com.splicemachine.tools.ResourcePool;
import com.splicemachine.tools.version.ManifestReader;
import com.splicemachine.tools.version.SpliceMachineVersion;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;
import com.splicemachine.utils.kryo.KryoPool;
import com.splicemachine.utils.logging.LogManager;
import com.splicemachine.utils.logging.Logging;
import com.splicemachine.uuid.Snowflake;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import com.splicemachine.db.drda.NetworkServerControl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.management.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A central/important class in our application. One instance per JVM.  Starts major services including network server
 * that listens for JDBC on port 1527, coordinates initial schema creation, etc.
 *
 * @author Scott Fines
 *         Created on: 3/1/13
 */
public class SpliceDriver {

    private static final String ENDPOINT_CLASS_NAME = "com.splicemachine.derby.hbase.SpliceIndexEndpoint";
    private static final Logger LOG = Logger.getLogger(SpliceDriver.class);
    private static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private static final TempTable tempTable = new TempTable(SpliceConstants.TEMP_TABLE_BYTES);
    private static final SpliceMachineVersion spliceVersion = new ManifestReader().createVersion();
    private static volatile SpliceDriver INSTANCE;

    private final AtomicReference<State> stateHolder = new AtomicReference<>(State.NOT_STARTED);
    private final MetricsRegistry spliceMetricsRegistry = new MetricsRegistry();
    private final List<Service> services = new CopyOnWriteArrayList<>();
    private final Properties props = new Properties();
    private final SnowflakeLoader snowLoader = new SnowflakeLoader();
    private final TaskMonitor taskMonitor;
    private final JobScheduler jobScheduler;
    private final TaskScheduler threadTaskScheduler;
    private final ExecutorService lifecycleExecutor;
    private final WriteCoordinator writeCoordinator;
    private final DDLWatcher ddlWatcher;

    private RegionServerServices regionServerServices;
    private JmxReporter metricsReporter;
    private Connection connection;
    private XplainTaskReporter taskReporter;

    private volatile NetworkServerControl server;
    private volatile Snowflake snowflake;
    private StatementManager statementManager;
    private Logging logging;

    private final ResourcePool<SpliceSequence, AbstractSequenceKey> sequences = CachedResourcePool.
            Builder.<SpliceSequence, AbstractSequenceKey>newBuilder()
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .generator(new SpliceSequenceAbstractSequenceKeyGenerator()).build();

    public static SpliceDriver driver() {
        /* Lazy init so that unit test can create instance with mock fields that starts quickly. */
        if (INSTANCE == null) {
            synchronized (SpliceDriver.class) {
                if(INSTANCE==null) {
                    try {
                        INSTANCE = new SpliceDriver();
                    } catch (IOException e) {
                        throw new RuntimeException("Unable to start SpliceDriver", e);
                    }
                }
            }
        }
        return INSTANCE;
    }

    private SpliceDriver() throws IOException {
        this(
                new ZkTaskMonitor(SpliceConstants.zkSpliceTaskPath, ZkUtils.getRecoverableZooKeeper()),
                WriteCoordinator.create(SpliceUtils.config),
                DDLCoordinationFactory.getWatcher()
        );
    }

    protected SpliceDriver(ZkTaskMonitor taskMonitor, WriteCoordinator writeCoordinator, DDLWatcher ddlWatcher) {
        this.lifecycleExecutor = MoreExecutors.namedSingleThreadExecutor("splice-lifecycle-manager");
        this.taskMonitor = taskMonitor;
        this.ddlWatcher = ddlWatcher;
        this.writeCoordinator = writeCoordinator;
        props.put(EmbedConnection.INTERNAL_CONNECTION, "true");

        TieredTaskSchedulerSetup setup = SchedulerPriorities.INSTANCE.getSchedulerSetup();
        TieredTaskScheduler.OverflowHandler overflowHandler = new SpliceDriverOverflowHandler();
        StealableTaskScheduler<RegionTask> overflowScheduler = new ExpandingTaskScheduler<>();
        threadTaskScheduler = new TieredTaskScheduler(setup, overflowHandler, overflowScheduler);
        jobScheduler = new DistributedJobScheduler(ZkUtils.getZkManager(), SpliceUtils.config);

        /* Make version information available to code in the derby codebase. */
        System.setProperty(Property.SPLICE_RELEASE, spliceVersion.getRelease());
        System.setProperty(Property.SPLICE_VERSION_HASH, spliceVersion.getImplementationVersion());
        System.setProperty(Property.SPLICE_BUILD_TIME, spliceVersion.getBuildTime());
        System.setProperty(Property.SPLICE_URL, spliceVersion.getURL());
        setBackupHFileCleaner();
    }

    private void setBackupHFileCleaner() {
        Configuration c = SpliceConstants.config;
        Set<String> hfileCleaners = new HashSet<String>();
        String[] cleaners = c.getStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);

        if (cleaners != null) {
            Collections.addAll(hfileCleaners, cleaners);
        }

        hfileCleaners.add(BackupHFileCleaner.class.getName());
        c.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
                hfileCleaners.toArray(new String[hfileCleaners.size()]));
    }
    public XplainTaskReporter getTaskReporter() {
        return taskReporter;
    }

    /**
     * Return the version of the Splice Machine software that is running.
     * This is not the version of the data dictionary which may be lower than the software version if the dictionary has not been upgraded yet.
     *
     * @return version of the Splice Machine software
     */
    public SpliceMachineVersion getVersion() {
        return spliceVersion;
    }

    public StatementManager getStatementManager() {
        return statementManager;
    }

    public ZkTaskMonitor getTaskMonitor() {
        return (ZkTaskMonitor) taskMonitor;
    }

    public WriteCoordinator getTableWriter() {
        return writeCoordinator;
    }

    public Properties getProperties() {
        return props;
    }

    public TempTable getTempTable() {
        return tempTable;
    }

    public <T extends Task> TaskScheduler<T> getTaskScheduler() {
        return (TaskScheduler<T>) threadTaskScheduler;
    }

    public TaskSchedulerManagement getTaskSchedulerManagement() {
        //this only works IF threadTaskScheduler implements the interface!
        return (TaskSchedulerManagement) threadTaskScheduler;
    }

    public <J extends CoprocessorJob> JobScheduler<J> getJobScheduler() {
        return (JobScheduler<J>) jobScheduler;
    }

    public void registerService(Service service) {
        // Start the service on the calling thread if we are registering it after service initialization.
        synchronized (this.services) {
            if (stateHolder.get().isPostServiceStartState()) {
                service.start();
            }
            this.services.add(service);
        }
    }

    public void deregisterService(Service service) {
        this.services.remove(service);
    }

    private HRegion getOnlineRegion(String encodedRegionName) {
        return regionServerServices == null ? null : regionServerServices.getFromOnlineRegions(encodedRegionName);
    }

    private RegionCoprocessorHost getCoprocessorHost(String encodedRegionName) {
        HRegion region = getOnlineRegion(encodedRegionName);
        return region == null ? null : region.getCoprocessorHost();
    }

    private CoprocessorEnvironment getSpliceIndexEndpointEnvironment(String encodedRegionName) {
        RegionCoprocessorHost host = getCoprocessorHost(encodedRegionName);
        return host == null ? null : host.findCoprocessorEnvironment(ENDPOINT_CLASS_NAME);
    }

    public SpliceBaseIndexEndpoint getIndexEndpoint(String encodedRegionName) {
        CoprocessorEnvironment ce = getSpliceIndexEndpointEnvironment(encodedRegionName);
        return ce == null ? null : ((IndexEndpoint) ce.getInstance()).getBaseIndexEndpoint();
    }

    public RegionWritePipeline getWritePipeline(String encodedRegionName) {
        SpliceBaseIndexEndpoint endpoint = getIndexEndpoint(encodedRegionName);
        if (endpoint == null) return null;
        return endpoint.getWritePipeline();
    }

    public boolean isStarted() {
        return stateHolder.get() == State.RUNNING;
    }

    public void start(RegionServerServices regionServerServices) {
        this.regionServerServices = regionServerServices;
        if (stateHolder.compareAndSet(State.NOT_STARTED, State.INITIALIZING)) {
            lifecycleExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        SpliceLogUtils.info(LOG, "Booting the SpliceDriver");

                        registerDebugTools();

                        ddlWatcher.start();

                        writeCoordinator.start();

                        //all we have to do is create it, it will register itself for us
                        new SpliceMetrics();

                        SpliceLogUtils.debug(LOG, "Booting Database");
                        boolean setRunning = bootDatabase();
                        if (!setRunning) {
                            abortStartup();
                            return null;
                        }

                        taskReporter = new XplainTaskReporter();
                        statementManager = new StatementManager();
                        logging = new LogManager();
                        SpliceLogUtils.debug(LOG, "Finished Booting Database");

                        //register JMX items --have to wait for the db to boot first
                        registerJMX();

                        SpliceLogUtils.debug(LOG, "Starting Services");
                        setRunning = startServices();
                        SpliceLogUtils.debug(LOG, "Done Starting Services");
                        if (!setRunning) {
                            abortStartup();
                            return null;
                        }

                        // Write the Splice version to the log.
                        SpliceLogUtils.info(LOG, String.format("Splice Machine Release = %s", spliceVersion.getRelease()));
                        SpliceLogUtils.info(LOG, String.format("Splice Machine Version Hash = %s", spliceVersion.getImplementationVersion()));
                        SpliceLogUtils.info(LOG, String.format("Splice Machine Build Time = %s", spliceVersion.getBuildTime()));
                        SpliceLogUtils.info(LOG, String.format("Splice Machine URL = %s", spliceVersion.getURL()));

                        SpliceLogUtils.debug(LOG, "Starting Server");
                        setRunning = startServer();
                        SpliceLogUtils.debug(LOG, "Done Starting Server");
                        if (!setRunning) {
                            abortStartup();
                            return null;
                        } else {
                            stateHolder.set(State.RUNNING);
                        }
                        return null;
                    } catch (Exception e) {
                        SpliceLogUtils.error(LOG, "Unable to boot Splice Machine", e);
                        ErrorReporter.get().reportError(SpliceDriver.class, e);
                        throw e;
                    }
                }
            });
        }
    }

    //registers configured debug and/or testing tools
    private void registerDebugTools() {
        if (!SpliceConstants.debugFailTasksRandomly) return; //nothing to do

        final double testTaskFailureRate = SpliceConstants.debugTaskFailureRate;
        if (testTaskFailureRate <= 0 || testTaskFailureRate > 1)
            return; //don't fail anything if the rate is out of our range

        final Random random = new Random(System.currentTimeMillis());
        SchedulerTracer.registerTaskStart(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                if (random.nextDouble() < testTaskFailureRate)
                    throw new Exception("Intentional task invalidation");
                return null;
            }
        });
    }

    private boolean bootDatabase() throws Exception {
        HBaseAdmin admin = null;
        connection = null;
        try {
            // The HBaseAdmin creates a connection to the ZooKeeper ensemble and thereafter into the HBase cluster.
            admin = SpliceUtilities.getAdmin(SpliceConstants.config);
            HTableDescriptor desc = new HTableDescriptor(SpliceMasterObserver.INIT_TABLE);
            desc.addFamily(new HColumnDescriptor(Bytes.toBytes("FOO")));
            // Create the special "SPLICE_INIT" table which triggers the creation of the SpliceMasterObserver and ultimately
            // triggers the creation of the "SPLICE_*" HBase tables.  This is an asynchronous call and so we "loop" via a
            // "please hold" exception and a recursive call to bootDatabase() along with a sleep.
            admin.createTable(desc);

            return false;
        } catch (PleaseHoldException pe) {
            Thread.sleep(5000);
            SpliceLogUtils.info(LOG, "Waiting for splice schema creation.");
            return bootDatabase();
        } catch (Exception e) {
            if (derbyFactory.getExceptionHandler().isPleaseHoldException(e)) {
                Thread.sleep(5000);
                SpliceLogUtils.info(LOG, "Waiting for splice schema creation");
                return bootDatabase();
            } else {
                // The exception signaling the start of a successfully running Splice Engine will be a SpliceDoNotRetryIOException.
                // When this is returned, it means that a connection to HBase and the creation (or previous existence) of the
                // "SPLICE_*" tables in HBase has been successful.
                // Not exactly sure why we are catching a generic Exception above.  Possibly there are other valid exceptions???

                /*
                 * If an upgrade has been forced, we are now finished with it since this bit of code here only runs
                 * on the region servers and we don't ever run upgrade code on region servers.  Only the master server
                 * runs upgrade code via the SpliceMasterObserver.  So we mark the flag to false to ensure that the
                 * region servers don't run the upgrade.
                 */
                SpliceConstants.upgradeForced = false;

                // Ensure ZK paths exist.
                ZkUtils.safeInitializeZooKeeper();
                // Initialize the table pool so the UUID generator below can access the SPLICE_SEQUENCES table in HBase.
                new SpliceAccessManager();
                // Since SPLICE_SEQUENCES table is set up, initialize the UUID generator, so the new Derby connection below
                // can execute an upgrade process if requested and create and store new system objects in the data dictionary tables.
                loadUUIDGenerator(SpliceConstants.config.getInt(HConstants.REGIONSERVER_PORT, 60020));

                // Create an embedded connection to Derby.  This essentially boots up Derby by creating an internal connection to it.
                // External connections to Derby are created later when the Derby network server is started.
                EmbedConnectionMaker maker = new EmbedConnectionMaker();
                connection = maker.createNew();
                return true;
            }
        } finally {
            Closeables.close(admin, true);
        }
    }

    public ResourcePool<SpliceSequence, AbstractSequenceKey> getSequencePool() {
        return sequences;
    }

    public void shutdown() {
        lifecycleExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    SpliceLogUtils.info(LOG, "Shutting down connections");
                    if (server != null) server.shutdown();

                    SpliceLogUtils.info(LOG, "Shutting down services");
                    for (Service service : services) {
                        service.shutdown();
                    }

                    if (metricsReporter != null) metricsReporter.shutdown();
                    SpliceLogUtils.info(LOG, "Destroying internal Engine");
                    stateHolder.set(State.SHUTDOWN);
                } catch (Exception e) {
                    SpliceLogUtils.error(LOG,
                            "Unable to shut down properly, this may affect the next time the service is started", e);
                }
                return null;
            }
        });
    }

    private void registerJMX() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {

            ObjectName statementInfoName = new ObjectName("com.splicemachine.statement:type=StatementManagement");
            mbs.registerMBean(statementManager, statementInfoName);
            ObjectName loggingInfoName = new ObjectName("com.splicemachine.utils.logging:type=LogManager");
            mbs.registerMBean(logging, loggingInfoName);

            writeCoordinator.registerJMX(mbs);

            SpliceBaseIndexEndpoint.registerJMX(mbs);

            new ManifestReader().registerJMX(mbs);

            //registry metricsRegistry
            metricsReporter = new JmxReporter(spliceMetricsRegistry);
            metricsReporter.start();

            //register TaskScheduler
            ((TieredTaskScheduler) threadTaskScheduler).registerJMX(mbs);

            //register TaskMonitor
            ObjectName taskMonitorName = new ObjectName("com.splicemachine.job:type=TaskMonitor");
            mbs.registerMBean(taskMonitor, taskMonitorName);

            //register JobScheduler
            ObjectName jobSchedulerName = new ObjectName("com.splicemachine.job:type=JobSchedulerManagement");
            mbs.registerMBean(jobScheduler.getJobMetrics(), jobSchedulerName);

            //register transaction stuff
            ObjectName rollForwardName = new ObjectName("com.splicemachine.txn:type=RollForwardManagement");
            mbs.registerMBean(TransactionalRegions.getRollForwardManagement(), rollForwardName);

            ObjectName txnStoreName = new ObjectName("com.splicemachine.txn:type=TxnStoreManagement");
            mbs.registerMBean(TransactionStorage.getTxnStoreManagement(), txnStoreName);

        } catch (MalformedObjectNameException | NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
            //we want to log the message, but this shouldn't affect startup
            SpliceLogUtils.error(LOG, "Unable to register JMX entries", e);
        }
    }

    protected boolean startServices() {
        /* atomically start services and update state to INITIALIZED_SERVICES */
        synchronized (this.services) {
            try {
                SpliceLogUtils.info(LOG, "Splice Engine is Running, Enabling Services");
                boolean started = true;
                for (Service service : services) {
                    started = started && service.start();
                }
                return started;
            } catch (Exception e) {
                //just in case the outside services decide to blow up on me
                SpliceLogUtils.error(LOG, "Unable to start services, aborting startup", e);
                return false;
            } finally {
                stateHolder.set(State.INITIALIZED_SERVICES);
            }
        }
    }

    private void abortStartup() {
        stateHolder.set(State.STARTUP_FAILED);
    }

    private boolean startServer() {
        SpliceLogUtils.info(LOG, "Services successfully started, enabling JDBC connections...");
        try {
            server = new NetworkServerControl(InetAddress.getByName(SpliceConstants.derbyBindAddress), SpliceConstants.derbyBindPort);
            server.setLogConnections(true);
            server.start(new DerbyOutputLoggerWriter());
            SpliceLogUtils.info(LOG, "Ready to accept JDBC connections on %s:%s", SpliceConstants.derbyBindAddress, SpliceConstants.derbyBindPort);
            return true;
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, "Unable to start Client/Server Protocol", e);
            return false;
        }
    }

    public Snowflake getUUIDGenerator() {
        return snowflake;
    }

    public void loadUUIDGenerator(int port) throws IOException {
        snowflake = snowLoader.load(port);
    }

    public MetricsRegistry getRegistry() {
        return spliceMetricsRegistry;
    }

    public Connection getInternalConnection() {
        return connection;
    }

    public static KryoPool getKryoPool() {
        return SpliceKryoRegistry.getInstance();
    }

    private static enum State {
        NOT_STARTED, INITIALIZING, INITIALIZED_SERVICES, RUNNING, STARTUP_FAILED, SHUTDOWN;

        public boolean isPostServiceStartState() {
            return INITIALIZED_SERVICES.equals(this) || RUNNING.equals(this);
        }
    }

    private static class SpliceSequenceAbstractSequenceKeyGenerator implements ResourcePool.Generator<SpliceSequence, AbstractSequenceKey> {
        @Override
        public SpliceSequence makeNew(AbstractSequenceKey refKey) throws StandardException {
            return refKey.makeNew();
        }

        @Override
        public void close(SpliceSequence entity) throws Exception {
            entity.close();
        }
    }

    private static class SpliceDriverOverflowHandler implements TieredTaskScheduler.OverflowHandler {
        @Override
        public OverflowPolicy shouldOverflow(Task t) {
            return t.getParentTaskId() == null ? OverflowPolicy.ENQUEUE : OverflowPolicy.OVERFLOW;
        }
    }
}
