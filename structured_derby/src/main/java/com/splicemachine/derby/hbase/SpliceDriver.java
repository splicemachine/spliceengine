package com.splicemachine.derby.hbase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.derby.logging.DerbyOutputLoggerWriter;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.TableWriter;
import com.splicemachine.tools.ConnectionPool;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Created on: 3/1/13
 */
public class SpliceDriver {
    private static final Logger LOG = Logger.getLogger(SpliceDriver.class);
    private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String protocol = "jdbc:derby:splice:";
    private static final String dbName = "wombat";
    private final List<Service> services = new CopyOnWriteArrayList<Service>();
    private static final int DEFAULT_PORT = 1527;
    private static final String DEFAULT_SERVER_ADDRESS = "0.0.0.0";



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

    private AtomicReference<State> stateHolder = new AtomicReference<State>(State.NOT_STARTED);

    private volatile EmbedConnection conn;
    private volatile LanguageConnectionContext lcc;
    private volatile Properties props = new Properties();

    private volatile NetworkServerControl server;

    private volatile TableWriter writerPool;
    private volatile CountDownLatch initalizationLatch = new CountDownLatch(1);

    private ExecutorService executor;
    private ConnectionPool embeddedConnections;

    private SpliceDriver(){
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("splice-lifecycle-manager").build();
        executor = Executors.newSingleThreadExecutor(factory);


        //TODO -sf- create a separate pool for writing to TEMP
        try {
            writerPool = TableWriter.create(SpliceUtils.config);

            embeddedConnections = ConnectionPool.create(SpliceUtils.config);
        } catch (Exception e) {
            throw new RuntimeException("Unable to boot Splice Driver",e);
        }
    }

    public TableWriter getTableWriter() {
        return writerPool;
    }

    public Properties getProperties() {
        return props;
    }

    public ConnectionPool embedConnPool(){
        return embeddedConnections;
    }

    public LanguageConnectionContext getLanguageConnectionContext(){
        return getLanguageConnectionContext(3);
    }

    private LanguageConnectionContext getLanguageConnectionContext(int numAttempts) {
        if(numAttempts<0){
            throw new AssertionError("Unable to get Language Connection Context, " +
                    "Driver failed to start up after multiple attempts");
        }
        switch (stateHolder.get()) {
            case STARTUP_FAILED:
                throw new AssertionError("Service Startup failed, unable to acquire Language Connection Context");
            case SHUTDOWN:
                throw new AssertionError("Service is shutdown");
            case INITIALIZING:
                //need to block until the initialization state has ended
                try {
                    initalizationLatch.await();
                } catch (InterruptedException e) {
                    SpliceLogUtils.warn(LOG,"Interrupted while waiting for Splice Driver initialization",e);
                    //interrupted during wait, see if it's good
                    return getLanguageConnectionContext(numAttempts-1);
                }
            case NOT_STARTED:
                start();
                return getLanguageConnectionContext(numAttempts);
            default:
                /*
                 * Either the lcc is null or it's not. Either way, return that to the caller
                 */
                return lcc;
        }
    }

    public void registerService(Service service){
        this.services.add(service);
        //If the service is registered after we've successfully started up, let it know on the same
        //thread.
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

                    writerPool.start();
                    boolean setRunning = true;
                    setRunning = enableDriver();
                    if(!setRunning) {
                        abortStartup();
                        return null;
                    }
                    setRunning = ensureHBaseTablesPresent();
                    if(!setRunning) {
                        abortStartup();
                        return null;
                    }
                    setRunning = startServices();
                    if(!setRunning) {
                        abortStartup();
                        return null;
                    }
                    setRunning = startServer();
                    if(!setRunning) {
                        abortStartup();
                        return null;
                    } else
                        stateHolder.set(State.RUNNING);
                        initalizationLatch.countDown();
                    return null;
                }
            });
        }
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

                    SpliceLogUtils.info(LOG,"Destroying internal Engine");
                    try{
                        if(conn!=null)
                            conn.close();
                        conn = null;
                    }finally{
                        stateHolder.set(State.SHUTDOWN);
                    }
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

    private EmbeddedDriver loadDriver() throws Exception {
        Monitor.clearMonitor();
        SpliceLogUtils.trace(LOG,"Attempting to load the Derby Embedded Driver");
        return (EmbeddedDriver) Class.forName(DRIVER).newInstance();
    }

    private boolean ensureHBaseTablesPresent() {
        SpliceLogUtils.info(LOG, "Ensuring Required Hbase Tables are present");
        HBaseAdmin admin = null;
        try{
            admin = new HBaseAdmin(SpliceUtils.config);
            if(!admin.tableExists(TxnConstants.TEMP_TABLE_BYTES)){
                HTableDescriptor td = SpliceUtils.generateDefaultDescriptor(TxnConstants.TEMP_TABLE);
                admin.createTable(td);
                SpliceLogUtils.info(LOG,TxnConstants.TEMP_TABLE+" created");
            }
            return true;
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"Unable to set up HBase Tables",e);
            return false;
        }finally{
            if(admin!=null){
                try{
                    admin.close();
                } catch (IOException e) {
                    SpliceLogUtils.error(LOG,"Unable to close Hbase admin, this could be symptomatic of a deeper problem",e);
                }
            }
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

    private boolean enableDriver() {
        try{
            SpliceLogUtils.info(LOG, "Constructing Internal Database Engine");
            EmbeddedDriver driver = loadDriver();
            conn = (EmbedConnection)driver.connect(protocol+dbName+";create=true",props);
            lcc = conn.getLanguageConnection();
            return true;
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"Unable to boot internal driver, aborting Startup",e);
            return false;
        }
    }

    private void abortStartup() {
        stateHolder.set(State.STARTUP_FAILED);
    }

    private boolean startServer() {
        SpliceLogUtils.info(LOG, "Services successfully started, enabling Connections");
        try{
            String bindAddress = SpliceUtils.config.get("splice.server.address",DEFAULT_SERVER_ADDRESS);
            int bindPort = SpliceUtils.config.getInt("splice.server.port", DEFAULT_PORT);
            server = new NetworkServerControl(InetAddress.getByName(bindAddress),bindPort);
            server.setLogConnections(true);
            server.start(new DerbyOutputLoggerWriter());
//            server.setTimeSlice(100);
//            server.setMaxThreads(1000);

            SpliceLogUtils.info(LOG,"Ready to accept connections");
            return true;
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"Unable to start Client/Server Protocol",e);
            return false;
        }
    }
}
