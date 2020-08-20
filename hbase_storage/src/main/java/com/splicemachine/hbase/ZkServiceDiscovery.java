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
 *
 */

package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.ServiceDiscovery;
import com.splicemachine.access.configuration.HBaseConfiguration;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import splice.com.google.common.net.HostAndPort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZkServiceDiscovery implements ServiceDiscovery {

    @Override
    public void registerServer(HostAndPort hostAndPort) throws IOException {
        try {
            String rootPath = HConfiguration.getConfiguration().getSpliceRootPath() + HBaseConfiguration.SERVERS_PATH;
            ZkUtils.safeCreate(rootPath, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            String path = rootPath + "/" + hostAndPort.toString();
            ZkUtils.safeDelete(path, -1);

            ZkUtils.create(path, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void deregisterServer(HostAndPort hostAndPort) throws IOException {
        try {
            String rootPath = HConfiguration.getConfiguration().getSpliceRootPath() + HBaseConfiguration.SERVERS_PATH;
            String path = rootPath + "/" + hostAndPort.toString();
            ZkUtils.safeDelete(path, -1);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<HostAndPort> listServers() throws IOException {
        String rootPath = HConfiguration.getConfiguration().getSpliceRootPath() + HBaseConfiguration.SERVERS_PATH;
        List<String> children = ZkUtils.getChildren(rootPath, false);
        List<HostAndPort> servers = new ArrayList<>(children.size());
        for (String c : children) {
            servers.add(HostAndPort.fromString(c));
        }
        return servers;
    }
}
