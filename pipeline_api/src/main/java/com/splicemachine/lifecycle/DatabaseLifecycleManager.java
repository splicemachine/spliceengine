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

package com.splicemachine.lifecycle;

import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Responsible for constructing the correct startup/shutdown sequence of Splice.
 *
 * Startup has three phases:
 *
 * 1. Engine Boot
 * 2. Generic Boot
 * 3. Server Boot
 *
 * It is possible to register a service at any of the three phases.
 *
 * @author Scott Fines
 *         Date: 1/4/16
 */
public class DatabaseLifecycleManager{
    private static final Logger LOG= Logger.getLogger(DatabaseLifecycleManager.class);
    public static volatile DatabaseLifecycleManager INSTANCE;

    public enum State{
        NOT_STARTED,
        BOOTING_ENGINE,
        BOOTING_GENERAL_SERVICES,
        BOOTING_SERVER,
        STARTUP_FAILED,
        RUNNING,
        SHUTTING_DOWN,
        SHUTDOWN
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
    private final CountDownLatch startupLock = new CountDownLatch(1);
    private final CopyOnWriteArrayList<DatabaseLifecycleService> engineServices= new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<DatabaseLifecycleService> generalServices= new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<DatabaseLifecycleService> networkServices= new CopyOnWriteArrayList<>();

    private final Executor lifecycleExecutor;
    private volatile MBeanServer jmxServer;

    public static DatabaseLifecycleManager manager(){
        DatabaseLifecycleManager dlm = INSTANCE; //volatile read
        if(dlm==null){
            synchronized(DatabaseLifecycleManager.class){
                dlm = INSTANCE; //volatile read
                if(dlm==null)
                    dlm = INSTANCE = new DatabaseLifecycleManager(); //volatile write
            }
        }
        return dlm;
    }

    public DatabaseLifecycleManager(){
       this(Executors.newSingleThreadExecutor());
    }

    public DatabaseLifecycleManager(Executor lifecycleExecutor){
        this.lifecycleExecutor = lifecycleExecutor;
    }

    public void start(){
        if(!state.compareAndSet(State.NOT_STARTED,State.BOOTING_ENGINE)) return; //someone else is doing stuff

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
                case BOOTING_ENGINE:
                case BOOTING_GENERAL_SERVICES:
                case BOOTING_SERVER:
                    try{
                        startupLock.await();
                    }catch(InterruptedException ignored){
                       Thread.currentThread().interrupt();
                    }
                    shutdown();
                    return;
                case RUNNING:
                    shouldContinue = !state.compareAndSet(State.RUNNING,State.SHUTTING_DOWN);
                    break;
                default:
                    throw new IllegalStateException("Unexpected state: "+ g);
            }
        }while(shouldContinue);

        lifecycleExecutor.execute(new Shutdown());
    }

    public void registerNetworkService(DatabaseLifecycleService engineService) throws Exception{
        final State state=this.state.get();
        switch(state){
            case NOT_STARTED:
                break;
            case BOOTING_ENGINE:
            case BOOTING_GENERAL_SERVICES:
                networkServices.add(engineService);
                return;
            case BOOTING_SERVER:
            case RUNNING:
                engineService.start();
                break;
            case STARTUP_FAILED:
                throw new IllegalStateException("Unable to register service, startup failed");
            case SHUTTING_DOWN:
                break;
            case SHUTDOWN:
                return; //nothing to do, we are shutting down
        }
        networkServices.add(engineService);
    }

    public void registerEngineService(DatabaseLifecycleService engineService) throws Exception{
        switch(state.get()){
            case NOT_STARTED:
                break;
            case BOOTING_ENGINE:
            case BOOTING_GENERAL_SERVICES:
            case BOOTING_SERVER:
            case RUNNING:
                //we have booted the general services, so we can start this directly
                engineService.start();
                engineService.registerJMX(jmxServer);
                break;
            case STARTUP_FAILED:
                throw new IllegalStateException("Unable to register service, startup failed");
            case SHUTTING_DOWN:
                break;
            case SHUTDOWN:
                return; //nothing to do, we are shutting down
        }
        engineServices.add(engineService);
    }

    public void registerGeneralService(DatabaseLifecycleService service) throws Exception{
        switch(state.get()){
            case NOT_STARTED:
                break;
            case BOOTING_ENGINE:
                generalServices.add(service);
                return;
            case BOOTING_GENERAL_SERVICES:
            case BOOTING_SERVER:
            case RUNNING:
                //we have booted the general services, so we can start this directly
                service.start();
                service.registerJMX(jmxServer);
                break;
            case STARTUP_FAILED:
                throw new IllegalStateException("Unable to register service, startup failed");
            case SHUTTING_DOWN:
                break;
            case SHUTDOWN:
                return; //nothing to do, we are shutting down
        }
        generalServices.add(service);
    }

    public void deregisterGeneralService(DatabaseLifecycleService service){
        generalServices.remove(service);
    }

    public void deregisterEngineService(DatabaseLifecycleService service){
        engineServices.remove(service);
    }

    public void deregisterNetworkService(DatabaseLifecycleService service){
        networkServices.remove(service);
    }

    public void setJMXServer(MBeanServer mbs){
        this.jmxServer = mbs;
    }
    public void awaitStartup() throws InterruptedException{
        startupLock.await();
    }

    public State getState(){
        return state.get();
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class Startup implements Runnable{
        @Override
        public void run(){
            if(!bootServices(State.BOOTING_GENERAL_SERVICES,engineServices)) return; //bail, we encountered an error
            if(!bootServices(State.BOOTING_SERVER,generalServices)) return; //bail, we encountered an error
            bootServices(State.RUNNING,networkServices);
        }

        private boolean bootServices(State nextState,List<DatabaseLifecycleService> services){
            try{
                for(DatabaseLifecycleService service : services){
                    try{
                        service.start();
                    }catch(Exception e){
                        LOG.error("Error during during startup of service "+ service+":",e);
                        state.set(State.STARTUP_FAILED);
                        new Shutdown().run();//cleanly shut down services
                        return false;
                    }
                }
                //now register JMX
                state.set(nextState);
            }finally{
                startupLock.countDown(); //release any waiting threads
            }
            //register JMX
            if(jmxServer==null)
                jmxServer= ManagementFactory.getPlatformMBeanServer();
            for(DatabaseLifecycleService service: services){
                try{
                    service.registerJMX(jmxServer);
                }catch(Exception e){
                    LOG.error("Error during during JMX registration of service "+ service+":",e);
                    state.set(State.STARTUP_FAILED);
                    new Shutdown().run();//cleanly shut down services
                    return false;
                }
            }
            return true;
        }
    }

    private class Shutdown implements Runnable{

        @Override
        public void run(){
            //shut down network services first
            List<DatabaseLifecycleService> toShutdown = new ArrayList<>(networkServices);
            Collections.reverse(toShutdown); //reverse the shutdown order from the startup order
            for(DatabaseLifecycleService service:toShutdown){
                try{
                    service.shutdown();
                } catch(Exception e){
                    LOG.error("Error during shutdown of service "+ service+":",e);
                }
            }

            toShutdown.clear();
            toShutdown.addAll(generalServices);
            Collections.reverse(generalServices);
            for(DatabaseLifecycleService service:toShutdown){
                try{
                    service.shutdown();
                } catch(Exception e){
                    LOG.error("Error during shutdown of service "+ service+":",e);
                }
            }

            toShutdown.clear();
            toShutdown.addAll(engineServices);
            Collections.reverse(engineServices);
            for(DatabaseLifecycleService service:toShutdown){
                try{
                    service.shutdown();
                } catch(Exception e){
                    LOG.error("Error during shutdown of service "+ service+":",e);
                }
            }
        }
    }
}
