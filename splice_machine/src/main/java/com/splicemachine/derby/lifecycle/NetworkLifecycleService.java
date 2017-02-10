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

package com.splicemachine.derby.lifecycle;

import javax.management.MBeanServer;
import java.net.InetAddress;

import org.apache.log4j.Logger;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.drda.NetworkServerControl;
import com.splicemachine.derby.logging.DerbyOutputLoggerWriter;
import com.splicemachine.lifecycle.DatabaseLifecycleService;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class NetworkLifecycleService implements DatabaseLifecycleService{
    private static final Logger LOG=Logger.getLogger(NetworkLifecycleService.class);
    private final SConfiguration config;
    private NetworkServerControl server;

    public NetworkLifecycleService(SConfiguration config){
        this.config=config;
    }

    @Override
    public void start() throws Exception{
        try {

            String bindAddress = config.getNetworkBindAddress();
            int bindPort = config.getNetworkBindPort();
            server = new NetworkServerControl(InetAddress.getByName(bindAddress),bindPort);
            server.setLogConnections(true);
            server.start(new DerbyOutputLoggerWriter());
            SpliceLogUtils.info(LOG, "Ready to accept JDBC connections on %s:%s", bindAddress,bindPort);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, "Unable to start Client/Server Protocol", e);
            throw e;
        }

    }

    @Override
    public void registerJMX(MBeanServer mbs) throws Exception{
    }

    @Override
    public void shutdown() throws Exception{
        server.shutdown();
    }
}
