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

package com.splicemachine.ddl;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.ddl.DDLCommunicator;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 9/4/15
 */
public class ZooKeeperDDLCommunicator implements DDLCommunicator{
    private final DDLZookeeperClient zkClient;

    public ZooKeeperDDLCommunicator(DDLZookeeperClient zkClient){
        this.zkClient=zkClient;
    }

    @Override
    public String createChangeNode(DDLMessage.DDLChange change) throws StandardException{
        return zkClient.createChangeNode(change.toByteArray());
    }

    @Override
    public Collection<String> activeListeners(com.splicemachine.derby.ddl.CommunicationListener asyncListener) throws StandardException{
        Watcher listenerWatcher = new ActiveWatcher(asyncListener);
        return zkClient.getActiveServers(listenerWatcher);
    }

    @Override
    public Collection<String> completedListeners(String changeId,com.splicemachine.derby.ddl.CommunicationListener asyncListener) throws StandardException{
        Watcher childrenWatcher = new ChildWatcher(asyncListener);
        return zkClient.getFinishedServers(changeId,childrenWatcher);
    }

    @Override
    public String getErrorMessage(String changeId, String errorId) throws StandardException {
        return zkClient.getServerChangeData(changeId,errorId);
    }

    @Override
    public void deleteChangeNode(String changeId){
        zkClient.deleteChangeNode(changeId);
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
