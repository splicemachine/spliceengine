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

package com.splicemachine.hbase;

import com.splicemachine.access.api.GetActiveSessionsTask;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.eclipse.jetty.util.ConcurrentHashSet;
import splice.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class SessionsWatcherImpl implements com.splicemachine.si.api.session.SessionsWatcher {
    private static final Logger LOG = Logger.getLogger(SessionsWatcherImpl.class);
    private final Set<Long> activeSessions = new ConcurrentHashSet<>();

    public static final SessionsWatcherImpl INSTANCE = new SessionsWatcherImpl();

    private SessionsWatcherImpl(){}

    @Override
    public Set<Long> getLocalActiveSessions() {
        return new HashSet<>(activeSessions);
    }

    @Override
    public List<Long> getAllActiveSessions() {
        Set<Long> idSet = new HashSet<>(activeSessions);
        try {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "fetch all active sessions");

            PartitionAdmin pa = SIDriver.driver().getTableFactory().getAdmin();
            ExecutorService executorService = SIDriver.driver().getExecutorService();
            Collection<PartitionServer> servers = pa.allServers();

            List<Future<Set<Long>>> futures = Lists.newArrayList();
            for (PartitionServer server : servers) {
                GetActiveSessionsTask task = SIDriver.driver().getAllActiveSessionsTaskFactory().get(
                        server.getHostname(), server.getPort(), server.getStartupTimestamp());
                futures.add(executorService.submit(task));
            }

            for (Future<Set<Long>> future : futures) {
                Set<Long> localActiveSessions = future.get();
                idSet.addAll(localActiveSessions);
            }
        } catch (IOException | ExecutionException | InterruptedException e) {
            SpliceLogUtils.error(LOG, "Unable to fetch all active sessions. " +
                    "Leaving local active session ID list untouched. Error cause by: %s", e);
        }
        List<Long> result = new ArrayList<>(idSet);
        Collections.sort(result);
        return result;
    }

    @Override
    public void registerSession(long sessionId) {
        activeSessions.add(sessionId);
    }

    @Override
    public void unregisterSession(long sessionId) {
        activeSessions.remove(sessionId);
    }

}
