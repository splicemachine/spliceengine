package com.splicemachine.si.api.session;

import java.util.List;
import java.util.Set;

public interface SessionsWatcher {
    Set<String> getLocalActiveSessions();

    List<String> getAllActiveSessions();

    void registerSession(long machineID, String sessionId);

    void unregisterSession(long machineID, String sessionId);
}
