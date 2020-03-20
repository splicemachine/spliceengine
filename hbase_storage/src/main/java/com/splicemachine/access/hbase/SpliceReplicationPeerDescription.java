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

package com.splicemachine.access.hbase;

import com.splicemachine.access.api.ReplicationPeerDescription;

/**
 * Created by jyuan on 10/2/19.
 */
public class SpliceReplicationPeerDescription implements ReplicationPeerDescription{
    private final String id;
    private final boolean enabled;
    private final String clusterKey;
    private boolean serial;

    public SpliceReplicationPeerDescription(String id, String clusterKey, boolean enabled) {
        this.id = id;
        this.clusterKey = clusterKey;
        this.enabled = enabled;
    }

    public SpliceReplicationPeerDescription(String id, String clusterKey, boolean enabled, boolean serial) {
        this.id = id;
        this.clusterKey = clusterKey;
        this.enabled = enabled;
        this.serial = serial;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String getClusterKey() {
        return clusterKey;
    }

    @Override
    public boolean isSerial() {
        return serial;
    }
}
