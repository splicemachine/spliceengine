package com.splicemachine.lifecycle;

import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Responsible for constructing the correct startup/shutdown sequence of Splice.
 *
 * @author Scott Fines
 *         Date: 1/4/16
 */
public class DatabaseLifecycleManager{
    private static final Logger LOG= Logger.getLogger(DatabaseLifecycleManager.class);
    public static final DatabaseLifecycleManager INSTANCE = new DatabaseLifecycleManager();

    private enum State{
        NOT_STARTED,
        STARTING,
        STARTUP_FAILED,
        RUNNING,
        SHUTTING_DOWN,
        SERVICES_RUNNING,SHUTDOWN
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
    private final CountDownLatch startupLock = new CountDownLatch(1);
    private final CopyOnWriteArrayList<DatabaseLifecycleService> services = new CopyOnWriteArrayList<>();

    private final Executor lifecycleExecutor =Executors.newSingleThreadExecutor();
    private volatile MBeanServer jmxServer;

    public static DatabaseLifecycleManager manager(){
        return INSTANCE;
    }


    public void start(){
        if(!state.compareAndSet(State.NOT_STARTED,State.STARTING)) return; //someone else is doing stuff

        lifecycleExecutor.execute(new Startup());
    }

    public void shutdown(){
        boolean shouldContinue;
        do{
            State g = state.get();
            switch(g){
                case STARTUP_FAILED:
                case SHUTTING_DOWN:
                case NOT_STARTED:
                case SHUTDOWN:
                    return; //nothing to shutdown
                case STARTING:
                    try{
                        startupLock.await();
                    }catch(InterruptedException ignored){
                       Thread.currentThread().interrupt();
                    }
                    shutdown();
                    return;
                case SERVICES_RUNNING:
                case RUNNING:
                    shouldContinue = !state.compareAndSet(State.RUNNING,State.SHUTTING_DOWN);
                    break;
                default:
                    throw new IllegalStateException("Unexpected state: "+ g);
            }
        }while(shouldContinue);

        lifecycleExecutor.execute(new Shutdown());
    }

    public void registerService(DatabaseLifecycleService service) throws Exception{
        switch(state.get()){
            case NOT_STARTED:
                break;
            case STARTING:
                startupLock.await(); //wait for startup to complete or fail
                registerService(service);
                break;
            case RUNNING:
                service.start();
                service.registerJMX(jmxServer);
                break;
            case STARTUP_FAILED:
                throw new IllegalStateException("Unable to register service, startup failed");
            case SHUTTING_DOWN:
            case SHUTDOWN:
                return; //nothing to do, we are shutting down
        }
        services.add(service);
    }

    public void awaitStartup() throws InterruptedException{
        startupLock.await();
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class Startup implements Runnable{
        @Override
        public void run(){
            try{
                for(DatabaseLifecycleService service : services){
                    try{
                        service.start();
                    }catch(Exception e){
                        LOG.error("Error during during startup of service "+ service+":",e);
                        state.set(State.STARTUP_FAILED);
                        new Shutdown().run();//cleanly shut down services
                        return;
                    }
                }
                //now register JMX
                state.set(State.SERVICES_RUNNING);
            }finally{
                startupLock.countDown(); //release any waiting threads
            }
            //register JMX
            jmxServer= ManagementFactory.getPlatformMBeanServer();
            for(DatabaseLifecycleService service:services){
                try{
                    service.registerJMX(jmxServer);
                }catch(Exception e){
                    LOG.error("Error during during JMX registration of service "+ service+":",e);
                    state.set(State.STARTUP_FAILED);
                    new Shutdown().run();//cleanly shut down services
                }
            }
            state.set(State.RUNNING);
        }
    }

    private class Shutdown implements Runnable{

        @Override
        public void run(){
            for(DatabaseLifecycleService service:services){
                try{
                    service.shutdown();
                } catch(Exception e){
                    LOG.error("Error during shutdown of service "+ service+":",e);
                }
            }
        }
    }
}
