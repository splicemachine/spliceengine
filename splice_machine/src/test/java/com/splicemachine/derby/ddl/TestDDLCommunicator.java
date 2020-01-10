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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.ddl.DDLMessage.*;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 9/8/15
 */
class TestDDLCommunicator implements DDLCommunicator{
    final List<String> allServers;
    final List<String> completedServers;

    private final List<CommunicationListener> activeServerListeners = new ArrayList<>();
    private final Map<String,List<CommunicationListener>> completedListeners = new HashMap<String,List<CommunicationListener>>(){
        @Override
        public List<CommunicationListener> get(Object key){
            List<CommunicationListener> communicationListeners=super.get(key);
            if(communicationListeners==null){
                communicationListeners = new ArrayList<>();
                super.put((String)key,communicationListeners);
            }
            return communicationListeners;
        }
    };

    public TestDDLCommunicator(List<String> allServers){
        this.allServers=new ArrayList<>(allServers);
        this.completedServers = new ArrayList<>(allServers.size());
    }

    @Override
    public String createChangeNode(DDLChange change) throws StandardException{
        return change.toString();
    }

    @Override
    public Collection<String> activeListeners(CommunicationListener asyncListener) throws StandardException{
        activeServerListeners.add(asyncListener);
        return new ArrayList<>(allServers);
    }

    @Override
    public Collection<String> completedListeners(String changeId,CommunicationListener asyncListener) throws StandardException{
        completedListeners.get(changeId).add(asyncListener);
        return new ArrayList<>(completedServers);
    }

    @Override
    public void deleteChangeNode(String changeId){
        completedListeners.remove(changeId);
    }

    public void commissionServer(String server){
        allServers.add(server);
        for(CommunicationListener listener:activeServerListeners){
            listener.onCommunicationEvent(server);
        }
    }

    public void decommissionServer(String server){
        boolean remove=allServers.remove(server);
        if(remove){
            for(CommunicationListener listener:activeServerListeners){
                listener.onCommunicationEvent(server);
            }
        }
    }

    public void serverCompleted(String changeId,String server){
        this.completedServers.add(server);
        for(CommunicationListener listener:completedListeners.get(changeId)){
            listener.onCommunicationEvent(changeId);
        }
    }

    @Override
    public String getErrorMessage(String changeId, String errorId) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }
}
