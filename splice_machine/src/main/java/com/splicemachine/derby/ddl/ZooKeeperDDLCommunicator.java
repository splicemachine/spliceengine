package com.splicemachine.derby.ddl;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.ddl.DDLMessage;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 9/4/15
 */
public class ZooKeeperDDLCommunicator implements DDLCommunicator{
    @Override
    public String createChangeNode(DDLMessage.DDLChange change) throws StandardException{
        return DDLZookeeperClient.createChangeNode(change.toByteArray());
    }

    @Override
    public Collection<String> activeListeners(com.splicemachine.derby.ddl.CommunicationListener asyncListener) throws StandardException{
        Watcher listenerWatcher = new ActiveWatcher(asyncListener);
        return DDLZookeeperClient.getActiveServers(listenerWatcher);
    }

    @Override
    public Collection<String> completedListeners(String changeId,com.splicemachine.derby.ddl.CommunicationListener asyncListener) throws StandardException{
        Watcher childrenWatcher = new ChildWatcher(asyncListener);
        return DDLZookeeperClient.getFinishedServers(changeId,childrenWatcher);
    }

    @Override
    public void deleteChangeNode(String changeId){
        DDLZookeeperClient.deleteChangeNode(changeId);
    }


    /* ****************************************************************************************************************/
    /*private helper classes*/
    private class ActiveWatcher implements Watcher{
        private com.splicemachine.derby.ddl.CommunicationListener asyncListener;

        public ActiveWatcher(com.splicemachine.derby.ddl.CommunicationListener asyncListener){
            this.asyncListener=asyncListener;
        }

        @Override
        public void process(WatchedEvent watchedEvent){
           asyncListener.onCommunicationEvent(watchedEvent.getPath());
        }
    }

    private class ChildWatcher implements Watcher{
        private com.splicemachine.derby.ddl.CommunicationListener listener;

        public ChildWatcher(com.splicemachine.derby.ddl.CommunicationListener asyncListener){
            listener=asyncListener;
        }

        @Override
        public void process(WatchedEvent watchedEvent){
            if(watchedEvent.getType().equals(Event.EventType.NodeChildrenChanged))
                listener.onCommunicationEvent(watchedEvent.getPath());
        }
    }
}
