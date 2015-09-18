package com.splicemachine.derby.ddl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 9/4/15
 */
public class ZooKeeperDDLCommunicator implements DDLCommunicator{
    @Override
    public String createChangeNode(DDLChange change) throws StandardException{
        byte[] data= encode(change);
        return DDLZookeeperClient.createChangeNode(data);
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

    protected byte[] encode(DDLChange change) {
        KryoPool kp = SpliceKryoRegistry.getInstance();
        Kryo kryo = kp.get();
        byte[] data;
        try{
            Output output = new Output(128,-1);
            kryo.writeObject(output,change);
            data = output.toBytes();
        }finally{
            kp.returnInstance(kryo);
        }
        return data;
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
