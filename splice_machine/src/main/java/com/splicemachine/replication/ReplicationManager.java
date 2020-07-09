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

package com.splicemachine.replication;

import com.splicemachine.access.api.ReplicationPeerDescription;
import com.splicemachine.db.iapi.error.StandardException;

import java.util.List;

/**
 * Created by jyuan on 2/6/19.
 */
public interface ReplicationManager {

    void addPeer(short peerId, String peerClusterKey) throws StandardException;
    void removePeer(short peerId) throws StandardException;
    void enablePeer(String clusterKey, short peerId, String peerClusterKey) throws StandardException;
    void disablePeer(short peerId) throws StandardException;
    void enableTableReplication(String tableName) throws StandardException;
    void disableTableReplication(String tableName) throws StandardException;
    void setReplicationRole(String role) throws StandardException;
    String getReplicationRole() throws StandardException;
    List<ReplicationPeerDescription> getReplicationPeers() throws StandardException;
    void monitorReplication(String primaryClusterKey, String replicaClusterKey) throws StandardException;
    default String getReplicatedWalPosition(String wal) throws StandardException{ return null;}
    default List<String> getReplicatedWalPositions(short peerId) throws StandardException{return null;}
    default long getReplicationProgress() throws StandardException {return -1;};
    String dumpUnreplicatedWals() throws StandardException;
}
