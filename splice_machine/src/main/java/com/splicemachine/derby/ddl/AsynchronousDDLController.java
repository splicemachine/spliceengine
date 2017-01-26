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

package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.DDLConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.LockFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;

/**
 * Used on the node where DDL changes are initiated to communicate changes to other nodes.
 */
public class AsynchronousDDLController implements DDLController, CommunicationListener {

    private static final Logger LOG = Logger.getLogger(AsynchronousDDLController.class);

    // timeout to refresh the info, in case some server is dead or a new server came up
    private final long refreshInterval;
    // maximum wait for everybody to respond, after this we fail the DDL change
    private final long maximumWaitTime;

    private final ActiveServerList activeServers = new ActiveServerList();
    private final DDLCommunicator communicator;
    private final Clock clock;
    private final Lock notificationLock;
    private final Condition notificationSignal;

    public AsynchronousDDLController(DDLCommunicator communicator,
                                     LockFactory lockFactory,
                                     Clock clock,
                                     long refreshInterval,
                                     long maximumWaitTime){
        this.communicator=communicator;
        this.clock = clock;
        this.notificationLock = lockFactory.newLock();
        this.notificationSignal = notificationLock.newCondition();
        this.refreshInterval= refreshInterval;
        this.maximumWaitTime= maximumWaitTime;
    }

    public AsynchronousDDLController(DDLCommunicator communicator,
                                     LockFactory lockFactory,
                                     Clock clock,
                                     SConfiguration configuration){
        this.communicator=communicator;
        this.clock = clock;
        this.notificationLock = lockFactory.newLock();
        this.notificationSignal = notificationLock.newCondition();
        this.refreshInterval= configuration.getDdlRefreshInterval();
        this.maximumWaitTime= configuration.getMaxDdlWait();
        assert this.refreshInterval > 0 : "Must have greater than zero asynchronous refresh interval";
    }

    @Override
    public String notifyMetadataChange(DDLChange change) throws StandardException {
        String changeId = communicator.createChangeNode(change);

        long availableTime =maximumWaitTime;
        long elapsedTime = 0;
        Collection<String> finishedServers =Collections.emptyList();
        Collection<String> activeServers = Collections.emptyList();
        while (availableTime>0) {
            activeServers = this.activeServers.getActiveServers();
            finishedServers = communicator.completedListeners(changeId,this);
            if (finishedServers.containsAll(activeServers)) {
                // everybody responded, leave loop
                return changeId;
            }

            for (String finishedServer: finishedServers) {
                if (finishedServer.startsWith(DDLConfiguration.ERROR_TAG)) {
                    String errorMessage = communicator.getErrorMessage(changeId,finishedServer);
                    throw StandardException.plainWrapException(new IOException(errorMessage));
                }
            }

            long startTimestamp = clock.currentTimeMillis();
            notificationLock.lock();
            try {
                notificationSignal.await(refreshInterval,TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw Exceptions.parseException(e);
            }finally{
                notificationLock.unlock();
            }
            long stopTimestamp = clock.currentTimeMillis();
            availableTime-= (stopTimestamp -startTimestamp);
            elapsedTime+=(stopTimestamp-startTimestamp);
        }

        logMissingServers(activeServers,finishedServers);
        communicator.deleteChangeNode(changeId);
        throw ErrorState.DDL_TIMEOUT.newException(elapsedTime,maximumWaitTime);
    }


    @Override
    public void finishMetadataChange(String changeId) throws StandardException {
        LOG.debug("Finishing metadata change with id " + changeId);
        communicator.deleteChangeNode(changeId);
    }

    @Override
    public void onCommunicationEvent(String node){
        notificationLock.lock();
        try{
            notificationSignal.signalAll();
        }finally{
            notificationLock.unlock();
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void logMissingServers(Collection<String> activeServers,Collection<String> finishedServers){
        Collection<String> missingServers = new LinkedList<>();
        for(String activeServer:activeServers){
            boolean found = false;
            for(String finishedServer:finishedServers){
                if(finishedServer.equals(activeServer)){
                    found = true;
                    break;
                }
            }
            if(!found)
                missingServers.add(activeServer);
        }
        LOG.error("Maximum wait for all servers exceeded. Missing response from "+missingServers);
        if(LOG.isDebugEnabled()){
            LOG.info("Full Server List:"+activeServers+", Responded Servers: "+ finishedServers);

        }
    }

    private class ActiveServerList implements CommunicationListener{
        private volatile boolean needsRefreshed = true;
        private volatile Collection<String> activeServers;

        public Collection<String> getActiveServers() throws StandardException {
            if(needsRefreshed){
                synchronized (this){
                    if(needsRefreshed){
                        activeServers = communicator.activeListeners(this);
                        needsRefreshed = false;
                    }
                }
            }
            return activeServers;
        }

        @Override
        public void onCommunicationEvent(String node){
            needsRefreshed = true;
            /*
             * If we receive notification that the active server list has changed, then
             * we want to break out of our wait loop and retry the coordination loop to make sure that
             * we are re-checking the server list (to deal with decommissions)
             */
            AsynchronousDDLController.this.onCommunicationEvent(node);
        }

        @Override public String toString(){ return activeServers==null? "[]": activeServers.toString(); }
    }

}
