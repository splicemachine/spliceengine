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

import com.splicemachine.access.hbase.SpliceReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;

/**
 * Created by jyuan on 12/4/19.
 */
public class HBaseReplicationPlatformUtil {

    public static ReplicationPeerConfig createReplicationConfig(String clusterKey) {
        ReplicationPeerConfig config = ReplicationPeerConfig.newBuilder()
                .setClusterKey(clusterKey)
                .setReplicationEndpointImpl("com.splicemachine.replication.SpliceInterClusterReplicationEndpoint")
                .build();
        return config;
    }

    public static SpliceReplicationPeerDescription getReplicationPeerDescription(org.apache.hadoop.hbase.replication.ReplicationPeerDescription peer) {
        SpliceReplicationPeerDescription replicationPeerDescription =
                new SpliceReplicationPeerDescription(peer.getPeerId(), peer.getPeerConfig().getClusterKey(),
                        peer.isEnabled());

        return replicationPeerDescription;
    }
}
