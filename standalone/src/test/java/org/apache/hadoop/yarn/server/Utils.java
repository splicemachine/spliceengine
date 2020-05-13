/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 *  version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package org.apache.hadoop.yarn.server;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Utils {
    private static final Logger LOG = Logger.getLogger(Utils.class);

    public static ResourceTracker getResourceTracker(ResourceTrackerService rt) {
        return new ResourceTracker() {

            @Override
            public NodeHeartbeatResponse nodeHeartbeat(
                    NodeHeartbeatRequest request) throws YarnException,
                    IOException {
                NodeHeartbeatResponse response;
                try {
                    response = rt.nodeHeartbeat(request);
                } catch (YarnException e) {
                    LOG.info("Exception in heartbeat from node " +
                            request.getNodeStatus().getNodeId(), e);
                    throw e;
                }
                return response;
            }

            @Override
            public RegisterNodeManagerResponse registerNodeManager(
                    RegisterNodeManagerRequest request)
                    throws YarnException, IOException {
                RegisterNodeManagerResponse response;
                try {
                    response = rt.registerNodeManager(request);

                } catch (NullPointerException npe) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ie) {
                        throw new IOException(ie);
                    }
                    return registerNodeManager(request);
                } catch (YarnException e) {
                    LOG.info("Exception in node registration from "
                            + request.getNodeId().toString(), e);
                    throw e;
                }
                return response;
            }

            @Override
            public UnRegisterNodeManagerResponse unRegisterNodeManager(
                    UnRegisterNodeManagerRequest request) throws YarnException, IOException {
                UnRegisterNodeManagerResponse response;
                try {
                    response = rt.unRegisterNodeManager(request);
                } catch (YarnException e) {
                    LOG.info("Exception in node registration from "
                            + request.getNodeId().toString(), e);
                    throw e;
                }
                return response;
            }
        };
    }

    public static void waitForNMToRegister(NodeManager nm) throws Exception{
        NMTokenSecretManagerInNM nmTokenSecretManagerNM =
                nm.getNMContext().getNMTokenSecretManager();
        NMContainerTokenSecretManager containerTokenSecretManager = nm.getNMContext().getContainerTokenSecretManager();
        int attempt = 60;
        while(attempt-- > 0) {
            try {
                if (nmTokenSecretManagerNM.getCurrentKey() != null && containerTokenSecretManager.getCurrentKey() != null) {
                    break;
                }
            } catch (Exception e) {

            }
            Thread.sleep(2000);
        }
    }
}
