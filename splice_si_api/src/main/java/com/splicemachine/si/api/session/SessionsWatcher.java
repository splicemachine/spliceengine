package com.splicemachine.si.api.session;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface SessionsWatcher {
    Set<String> getLocalActiveSessions();

    List<String> getAllActiveSessions();

    void registerSession(UUID sessionId);

    void unregisterSession(UUID sessionId);
}
