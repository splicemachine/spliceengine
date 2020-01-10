/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.splicemachine.EngineDriver;
import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.ReentrantLockFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.Pair;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MemDDLEnvironment implements DDLEnvironment{
    private DDLController ddlController;
    private DDLWatcher ddlWatcher;

    public MemDDLEnvironment(){ }

    @Override
    public DDLController getController(){
        return ddlController;
    }

    @Override
    public DDLWatcher getWatcher(){
        return ddlWatcher;
    }

    @Override
    public SConfiguration getConfiguration(){
        return SIDriver.driver().getConfiguration();
    }

    @Override
    public void configure(SqlExceptionFactory exceptionFactory,SConfiguration config) throws IOException{
        DDLChangeStore changeStore = new DDLChangeStore();
        DDLWatchChecker ddlWatchChecker=new DirectWatcher(changeStore);

        SIDriver driver = SIDriver.driver();
        this.ddlWatcher = new SynchronousDDLWatcher(
                        driver.readController(),
                        driver.getClock(),
                        driver.getConfiguration(),
                EngineDriver.driver().getExceptionFactory(),
                ddlWatchChecker,
                driver.getTxnSupplier());

        DDLCommunicator communicator = new DirectCommunicator(changeStore);
        this.ddlController = new AsynchronousDDLController(communicator,new ReentrantLockFactory(false),
                driver.getClock(),driver.getConfiguration());
        this.ddlWatcher.start();
    }

    private static class DDLChangeStore{
        final ConcurrentMap<String,DDLMessage.DDLChange> changesInFlight = new ConcurrentHashMap<>();
        final ConcurrentMap<String,DDLMessage.DDLChange> finishedChanges= new ConcurrentHashMap<>();
        final ConcurrentMap<String,String> errorChanges= new ConcurrentHashMap<>();
        volatile CommunicationListener watchListener;
        volatile CommunicationListener currentCoordinatorListener;
        private final AtomicLong changeCounter = new AtomicLong(0l);

        public String add(DDLMessage.DDLChange change){
            String label;
            if(change.hasChangeId()) //it probably is
                label = change.getChangeId();
            else
                label = change.getDdlChangeType()+"-"+change.getTxnId()+"-"+changeCounter.incrementAndGet();

            DDLMessage.DDLChange copy = change.toBuilder().setChangeId(label).build();
            changesInFlight.put(label,copy);
            CommunicationListener wl = watchListener;
            if(wl!=null)
                wl.onCommunicationEvent(label);
            return label;
        }

        public DDLMessage.DDLChange get(String changeId){
            return changesInFlight.get(changeId);
        }

        public void finish(Collection<Pair<DDLMessage.DDLChange, String>> processedChanges){
            CommunicationListener cl = currentCoordinatorListener;
            for(Pair<DDLMessage.DDLChange,String> change:processedChanges){
                String errorM = change.getSecond();
                DDLMessage.DDLChange ddlChange=change.getFirst();
                if(errorM!=null)
                    errorChanges.put(ddlChange.getChangeId(),errorM);
                else
                    finishedChanges.put(ddlChange.getChangeId(),ddlChange);
                if(cl!=null){
                    cl.onCommunicationEvent(change.getSecond());
                }
            }
        }

        public boolean isFinished(String changeId){
            return finishedChanges.containsKey(changeId)||errorChanges.containsKey(changeId);
        }

        public void remove(String changeId){
            finishedChanges.remove(changeId);
            errorChanges.remove(changeId);
            changesInFlight.remove(changeId);
        }

        public void kill(String key){
            remove(key);
        }
    }

    private static class DirectWatcher implements DDLWatchChecker{
        private final DDLChangeStore changeStore;

        public DirectWatcher(DDLChangeStore changeStore){
            this.changeStore=changeStore;
        }

        @Override
        public boolean initialize(CommunicationListener listener) throws IOException{
            changeStore.watchListener = listener;
            return true;
        }

        @Override
        public Collection<String> getCurrentChangeIds() throws IOException{
            return changeStore.changesInFlight.keySet();
        }

        @Override
        public DDLMessage.DDLChange getChange(String changeId) throws IOException{
            return changeStore.get(changeId);
        }

        @Override
        public void notifyProcessed(Collection<Pair<DDLMessage.DDLChange, String>> processedChanges) throws IOException{
            changeStore.finish(processedChanges);
        }

        @Override
        public void killDDLTransaction(String key){
            changeStore.kill(key);
        }
    }

    private static class DirectCommunicator implements DDLCommunicator{
        private final DDLChangeStore changeStore;
        private static final Collection<String> fauxServers=Collections.singletonList("in-mem");

        public DirectCommunicator(DDLChangeStore changeStore){
            this.changeStore=changeStore;
        }

        @Override
        public String createChangeNode(DDLMessage.DDLChange change) throws StandardException{
            return changeStore.add(change);
        }

        @Override
        public Collection<String> activeListeners(CommunicationListener asyncListener) throws StandardException{
            return fauxServers;
        }

        @Override
        public Collection<String> completedListeners(String changeId,CommunicationListener asyncListener) throws StandardException{
            changeStore.currentCoordinatorListener =asyncListener;
            if(changeStore.isFinished(changeId)){
                return fauxServers;
            }else return Collections.emptyList();
        }

        @Override
        public String getErrorMessage(String changeId,String errorId) throws StandardException{
            return changeStore.errorChanges.get(changeId);
        }

        @Override
        public void deleteChangeNode(String changeId){
            changeStore.remove(changeId);
        }
    }
}
