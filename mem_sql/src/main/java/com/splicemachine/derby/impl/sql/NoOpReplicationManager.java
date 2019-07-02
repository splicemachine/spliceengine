/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.replication.ReplicationManager;

/**
 * Created by jyuan on 2/6/19.
 */
public class NoOpReplicationManager implements ReplicationManager {
    private static NoOpReplicationManager ourInstance=new NoOpReplicationManager();

    public static NoOpReplicationManager getInstance(){
        return ourInstance;
    }

    private NoOpReplicationManager(){ }

    @Override
    public void addPeer(short peerId, String clusterKey) throws StandardException {

    }

    @Override
    public void removePeer(short peerId) throws StandardException {

    }

    @Override
    public void enablePeer(short peerId) {

    }

    @Override
    public void disablePeer(short peerId) {

    }

    @Override
    public void enableTableReplication(String tableName) {

    }

    @Override
    public void disableTableReplication(String tableName) {

    }

    @Override
    public void setupReplicationSink() throws StandardException {

    }

    @Override
    public void setupReplicationSinkLocal() throws StandardException {

    }

    @Override
    public void shutdownReplicationSink() throws StandardException {

    }

    @Override
    public void shutdownReplicationSinkLocal() throws StandardException {

    }
}
