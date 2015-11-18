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
}
