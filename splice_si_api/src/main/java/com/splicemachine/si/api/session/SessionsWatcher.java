package com.splicemachine.si.api.session;

import java.util.List;
import java.util.Set;

public interface SessionsWatcher {
    Set<Long> getLocalActiveSessions();

    List<Long> getAllActiveSessions();

    void registerSession(long sessionId);

    void unregisterSession(long sessionId);
}
