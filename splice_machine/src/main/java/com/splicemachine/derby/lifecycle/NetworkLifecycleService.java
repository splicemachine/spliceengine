/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
