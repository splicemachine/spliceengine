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

package com.splicemachine.storage;

import com.splicemachine.si.api.session.SessionsWatcher;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MSessionsWatcherImpl implements SessionsWatcher {
    private static final Logger LOG = Logger.getLogger(MSessionsWatcherImpl.class);
    private final Set<String> activeSessions = ConcurrentHashMap.newKeySet();

    public static final MSessionsWatcherImpl INSTANCE = new MSessionsWatcherImpl();

    private MSessionsWatcherImpl(){}

    @Override
    public Set<String> getLocalActiveSessions() {
        return activeSessions;
    }

    @Override
    public List<String> getAllActiveSessions() {
        return activeSessions.stream().sorted().collect(Collectors.toList());
    }

    @Override
    public void registerSession(String sessionId) {
        activeSessions.add(sessionId);
    }

    @Override
    public void unregisterSession(String sessionId) {
        activeSessions.remove(sessionId);
    }

}
