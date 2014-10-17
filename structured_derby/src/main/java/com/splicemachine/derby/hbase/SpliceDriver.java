package com.splicemachine.derby.hbase;

import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.ddl.DDLCoordinationFactory;
import com.splicemachine.derby.hbase.ManifestReader.SpliceMachineVersion;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.scheduler.*;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequenceKey;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.logging.DerbyOutputLoggerWriter;
import com.splicemachine.derby.management.StatementManager;
import com.splicemachine.derby.management.XplainTaskReporter;
import com.splicemachine.derby.utils.ErrorReporter;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.SpliceMetrics;
import com.splicemachine.hbase.writer.WriteCoordinator;
import com.splicemachine.job.*;
import com.splicemachine.si.api.TransactionStorage;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.tools.CachedResourcePool;
import com.splicemachine.tools.EmbedConnectionMaker;
import com.splicemachine.tools.ResourcePool;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;
import com.splicemachine.utils.kryo.KryoPool;
import com.splicemachine.utils.logging.LogManager;
import com.splicemachine.utils.logging.Logging;
import com.splicemachine.uuid.Snowflake;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.iapi.db.OptimizerTrace;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import javax.management.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A central/important class in our application. One instance per JVM.  Starts major services including network server
 * that listens for JDBC on port 1527, coordinates initial schema creation, etc.
 *
 * @author Scott Fines
 *         Created on: 3/1/13
 */
public class SpliceDriver extends SIConstants {
    private static final Logger LOG = Logger.getLogger(SpliceDriver.class);

    private final List<Service> services = new CopyOnWriteArrayList<Service>();
    private JmxReporter metricsReporter;
	private Connection connection;
	private XplainTaskReporter taskReporter;
	public XplainTaskReporter getTaskReporter() {
		return taskReporter;
	}

	public static enum State{
        NOT_STARTED,
        INITIALIZING,
        RUNNING,
        STARTUP_FAILED, SHUTDOWN
    }

    public static interface Service{
    	boolean start();
    	boolean shutdown();
    }

    private static final SpliceDriver INSTANCE = new SpliceDriver();
    private static SpliceMachineVersion spliceVersion;
    private AtomicReference<State> stateHolder = new AtomicReference<State>(State.NOT_STARTED);
    private volatile Properties props = new Properties();
    private volatile NetworkServerControl server;
    private volatile WriteCoordinator writerPool;
    private volatile CountDownLatch initalizationLatch = new CountDownLatch(1);
    private ExecutorService executor;
    private TaskScheduler threadTaskScheduler;
    private JobScheduler jobScheduler;
    private TaskMonitor taskMonitor;
	private SnowflakeLoader snowLoader;
    private volatile Snowflake snowflake;
    private final MetricsRegistry spliceMetricsRegistry = new MetricsRegistry();
	//-sf- when we need to, replace this with a list
	private final TempTable tempTable;
	private StatementManager statementManager;
    private Logging logging;

    private ResourcePool<SpliceSequence,SpliceSequenceKey> sequences = CachedResourcePool.
            Builder.<SpliceSequence,SpliceSequenceKey>newBuilder().expireAfterAccess(1l,TimeUnit.MINUTES).generator(new ResourcePool.Generator<SpliceSequence,SpliceSequenceKey>() {
        @Override
        public SpliceSequence makeNew(SpliceSequenceKey refKey) throws StandardException {
        	return refKey.makeNew();
        }

        @Override
        public void close(SpliceSequence entity) throws Exception{
            entity.close();
        }
    }).build();

    private SpliceDriver(){
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("splice-lifecycle-manager").build();
        executor = Executors.newSingleThreadExecutor(factory);
        props.put(EmbedConnection.INTERNAL_CONNECTION, "true");
        try {
            snowLoader = new SnowflakeLoader();
            writerPool = WriteCoordinator.create(SpliceUtils.config);
						TieredTaskSchedulerSetup setup = SchedulerPriorities.INSTANCE.getSchedulerSetup();
						TieredTaskScheduler.OverflowHandler overflowHandler = new TieredTaskScheduler.OverflowHandler() {
								@Override
								public OverflowPolicy shouldOverflow(Task t) {
										if(t.getParentTaskId()!=null) return OverflowPolicy.OVERFLOW;
										else return OverflowPolicy.ENQUEUE;
								}
						};
						StealableTaskScheduler<RegionTask> overflowScheduler =new ExpandingTaskScheduler<RegionTask>();
            threadTaskScheduler = new TieredTaskScheduler(setup,overflowHandler,overflowScheduler);
            jobScheduler = new DistributedJobScheduler(ZkUtils.getZkManager(),SpliceUtils.config);
            taskMonitor = new ZkTaskMonitor(SpliceConstants.zkSpliceTaskPath,ZkUtils.getRecoverableZooKeeper());
			tempTable = new TempTable(SpliceConstants.TEMP_TABLE_BYTES);
			initializeVersion();
        } catch (Exception e) {
            throw new RuntimeException("Unable to boot Splice Driver",e);
        }
    }

	/**
	 * Initialize the version of the Splice Machine software that is running.
	 */
	private void initializeVersion() {
		spliceVersion = new ManifestReader().createVersion();
		/*
		 * ------------------
		 * Hack alert...
		 * ------------------
		 * We need to get the Splice version included in most all exceptions thrown by Derby/Splice.
		 * This will help debugging stack traces when issues are filed by customers.
		 * And we've got our usual challenge of:
		 *     How do we get Splice info passed into Derby without moving a dozen classes from Splice into Derby?
		 * So we are setting the Splice version at boot time into Java system properties which will be accessed
		 * by the StandardException class in Derby.
		 */
		System.setProperty(Property.SPLICE_RELEASE, spliceVersion.getRelease());
		System.setProperty(Property.SPLICE_VERSION_HASH, spliceVersion.getImplementationVersion());
		System.setProperty(Property.SPLICE_BUILD_TIME, spliceVersion.getBuildTime());
		System.setProperty(Property.SPLICE_URL, spliceVersion.getURL());
	}

    /**
     * Return the version of the Splice Machine software that is running.
     * This is not the version of the data dictionary which may be lower than the software version if the dictionary has not been upgraded yet.
     * @return version of the Splice Machine software
     */
    public SpliceMachineVersion getVersion() {
        return spliceVersion;
    }

		public StatementManager getStatementManager(){ return statementManager; }
    public ZkTaskMonitor getTaskMonitor() { return (ZkTaskMonitor)taskMonitor; }

    public WriteCoordinator getTableWriter() {
        return writerPool;
    }

    public Properties getProperties() {
        return props;
    }

		public TempTable getTempTable() {
				return tempTable;
		}

		public <T extends Task> TaskScheduler<T> getTaskScheduler() {
        return (TaskScheduler<T>)threadTaskScheduler;
    }

    public TaskSchedulerManagement getTaskSchedulerManagement() {
        //this only works IF threadTaskScheduler implements the interface!
        return (TaskSchedulerManagement)threadTaskScheduler;
    }

    public <J extends CoprocessorJob> JobScheduler<J> getJobScheduler(){
        return (JobScheduler<J>)jobScheduler;
    }

    public void registerService(Service service){
        this.services.add(service);
        //If the service is registered after we've successfully started up, let it know on the same thread.
        if(stateHolder.get()==State.RUNNING)
            service.start();
    }

    public void deregisterService(Service service){
        this.services.remove(service);
    }

    public static SpliceDriver driver(){
        return INSTANCE;
    }

    public State getCurrentState(){
        return stateHolder.get();
    }

    public void start(){
        if(stateHolder.compareAndSet(State.NOT_STARTED,State.INITIALIZING)){
            executor.submit(new Callable<Void>(){
                @Override
                public Void call() throws Exception {
                    try{
                        SpliceLogUtils.info(LOG,"Booting the SpliceDriver");

                        registerDebugTools();

                        DDLCoordinationFactory.getWatcher().start();

                        writerPool.start();

                        //all we have to do is create it, it will register itself for us
                        SpliceMetrics metrics = new SpliceMetrics();

                        boolean setRunning = true;
                        SpliceLogUtils.debug(LOG, "Booting Database");
                        setRunning = bootDatabase();
                        if(!setRunning){
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
                        if(!setRunning) {
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
                        if(!setRunning) {
                            abortStartup();
                            return null;
                        } else
                            stateHolder.set(State.RUNNING);
                        initalizationLatch.countDown();
                        return null;
                    }catch(Exception e){
                        SpliceLogUtils.error(LOG,"Unable to boot Splice Machine",e);
                        ErrorReporter.get().reportError(SpliceDriver.class,e);
                        throw e;
                    }
                }
            });
        }
    }

    //registers configured debug and/or testing tools
    private void registerDebugTools() {
        if(!SpliceConstants.debugFailTasksRandomly) return; //nothing to do

        final double testTaskFailureRate = SpliceConstants.debugTaskFailureRate;
        if(testTaskFailureRate<=0||testTaskFailureRate>1)
            return; //don't fail anything if the rate is out of our range

        final Random random = new Random(System.currentTimeMillis());
        SchedulerTracer.registerTaskStart(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                if(random.nextDouble()<testTaskFailureRate)
                    throw new Exception("Intentional task invalidation");
                return null;
            }
        });
    }

    private boolean bootDatabase() throws Exception {
    	HBaseAdmin admin = null;
    	connection = null;
        try{
        	// The HBaseAdmin creates a connection to the ZooKeeper ensemble and thereafter into the HBase cluster.
        	admin = SpliceUtilities.getAdmin(config);
        	HTableDescriptor desc = new HTableDescriptor(SpliceMasterObserver.INIT_TABLE);
        	// Create the special "SPLICE_INIT" table which triggers the creation of the SpliceMasterObserver and ultimately
        	// triggers the creation of the "SPLICE_*" HBase tables.  This is an asynchronous call and so we "loop" via a
        	// "please hold" exception and a recursive call to bootDatabase() along with a sleep.
        	admin.createTable(desc);

        	return false;
        }  catch (PleaseHoldException pe) {
        	Thread.sleep(5000);
        	SpliceLogUtils.info(LOG, "Waiting for splice schema creation.");
        	return bootDatabase();
        } catch (Exception e) {
        	// The exception signaling the start of a successfully running Splice Engine will be a SpliceDoNotRetryIOException.
        	// When this is returned, it means that a connection to HBase and the creation (or previous existence) of the
        	// "SPLICE_*" tables in HBase has been successful.
        	// Not exactly sure why we are catching a generic Exception above.  Possibly there are other valid exceptions???

        	// Ensure ZK paths exist.
        	ZkUtils.safeInitializeZooKeeper();
        	// Initialize the table pool so the UUID generator below can access the SPLICE_SEQUENCES table in HBase.
        	new SpliceAccessManager();
        	// Since SPLICE_SEQUENCES table is set up, initialize the UUID generator, so the new Derby connection below
        	// can execute an upgrade process if requested and create and store new system objects in the data dictionary tables.
        	loadUUIDGenerator();

            // Create an embedded connection to Derby.  This essentially boots up Derby by creating an internal connection to it.
        	// External connections to Derby are created later when the Derby network server is started.
			EmbedConnectionMaker maker = new EmbedConnectionMaker();
			connection = maker.createNew();
			return true;
        } finally{
        	Closeables.close(admin,true);
        }
    }

    public ResourcePool<SpliceSequence,SpliceSequenceKey> getSequencePool(){
        return sequences;
    }

    public void shutdown(){
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try{
                    SpliceLogUtils.info(LOG,"Shutting down connections");
                    if(server!=null) server.shutdown();

                    SpliceLogUtils.info(LOG,"Shutting down services");
                    for(Service service:services){
                        service.shutdown();
                    }

                    if(metricsReporter!=null) metricsReporter.shutdown();
                    SpliceLogUtils.info(LOG,"Destroying internal Engine");
                    if(stateHolder!=null) stateHolder.set(State.SHUTDOWN);
                }catch(Exception e){
                    SpliceLogUtils.error(LOG,
                            "Unable to shut down properly, this may affect the next time the service is started",e);
                }
                return null;
            }
        });
    }

/********************************************************************************************/
    /*private helper methods*/

    private void registerJMX()  {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try{

						ObjectName statementInfoName = new ObjectName("com.splicemachine.statement:type=StatementManagement");
						mbs.registerMBean(statementManager,statementInfoName);
            ObjectName loggingInfoName = new ObjectName("com.splicemachine.utils.logging:type=LogManager");
            mbs.registerMBean(logging,loggingInfoName);

            writerPool.registerJMX(mbs);

            SpliceIndexEndpoint.registerJMX(mbs);

            new ManifestReader().registerJMX(mbs);
            
            //registry metricsRegistry
            metricsReporter = new JmxReporter(spliceMetricsRegistry);
            metricsReporter.start();

            //register error reporter
//            ObjectName errorReporterName = new ObjectName("com.splicemachine.error:type=ErrorReport");
//            mbs.registerMBean(ErrorReporter.get(), errorReporterName);

            //register TaskScheduler
						((TieredTaskScheduler)threadTaskScheduler).registerJMX(mbs);
//            ObjectName taskSchedulerName = new ObjectName("com.splicemachine.job:type=TaskSchedulerManagement");
//            mbs.registerMBean(threadTaskScheduler,taskSchedulerName);

            //register TaskMonitor
            ObjectName taskMonitorName = new ObjectName("com.splicemachine.job:type=TaskMonitor");
            mbs.registerMBean(taskMonitor,taskMonitorName);

            //register JobScheduler
            ObjectName jobSchedulerName = new ObjectName("com.splicemachine.job:type=JobSchedulerManagement");
            mbs.registerMBean(jobScheduler.getJobMetrics(),jobSchedulerName);

            //register transaction stuff
            ObjectName rollForwardName = new ObjectName("com.splicemachine.txn:type=RollForwardManagement");
            mbs.registerMBean(TransactionalRegions.getRollForwardManagement(),rollForwardName);

            ObjectName txnStoreName = new ObjectName("com.splicemachine.txn:type=TxnStoreManagement");
            mbs.registerMBean(TransactionStorage.getTxnStoreManagement(),txnStoreName);

        } catch (MalformedObjectNameException e) {
            //we want to log the message, but this shouldn't affect startup
            SpliceLogUtils.error(LOG,"Unable to register JMX entries",e);
        } catch (NotCompliantMBeanException e) {
            SpliceLogUtils.error(LOG, "Unable to register JMX entries", e);
        } catch (InstanceAlreadyExistsException e) {
            SpliceLogUtils.error(LOG, "Unable to register JMX entries", e);
        } catch (MBeanRegistrationException e) {
            SpliceLogUtils.error(LOG, "Unable to register JMX entries", e);
        }
    }

     private boolean startServices() {
        try{
            SpliceLogUtils.info(LOG, "Splice Engine is Running, Enabling Services");
            boolean started=true;
            for(Service service:services){
                started = started &&service.start();
            }
            return started;
        }catch(Exception e){
            //just in case the outside services decide to blow up on me
            SpliceLogUtils.error(LOG,"Unable to start services, aborting startup",e);
            return false;
        }
    }

    private void abortStartup() {
        stateHolder.set(State.STARTUP_FAILED);
    }

    private boolean startServer() {
        SpliceLogUtils.info(LOG, "Services successfully started, enabling JDBC connections...");
        try{
            server = new NetworkServerControl(InetAddress.getByName(derbyBindAddress),derbyBindPort);
            server.setLogConnections(true);
            server.start(new DerbyOutputLoggerWriter());
            SpliceLogUtils.info(LOG,"Ready to accept JDBC connections on %s:%s", derbyBindAddress, derbyBindPort);
            return true;
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"Unable to start Client/Server Protocol",e);
            return false;
        }
    }
    
    public Snowflake getUUIDGenerator(){
        return snowflake;
    }

    public void loadUUIDGenerator() throws IOException {
        snowflake =  snowLoader.load();
    }

    public MetricsRegistry getRegistry(){
        return spliceMetricsRegistry;
    }

		public Connection getInternalConnection(){
				return connection;
		}
    
    public static void setOptimizerTrace(boolean onOrOff) {
    	SpliceLogUtils.trace(LOG, "setOptimizerTrace %s", onOrOff);
    	OptimizerTrace.setOptimizerTrace(onOrOff);
    }

    public static void writeOptimizerTraceOutput(String filename) throws StandardException {
    	SpliceLogUtils.trace(LOG, "writeOptimizerTraceOutput %s", filename);
    	OptimizerTrace.writeOptimizerTraceOutputText(filename);
    }

    public static KryoPool getKryoPool(){
        return SpliceKryoRegistry.getInstance();
    }
}
