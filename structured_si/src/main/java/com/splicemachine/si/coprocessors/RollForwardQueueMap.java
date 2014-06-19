package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.RollForwardQueue;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import java.util.concurrent.ConcurrentMap;

public class RollForwardQueueMap {
    private static ConcurrentMap<String,RollForwardQueue> map  = new NonBlockingHashMap<String, RollForwardQueue>();

    public static void registerRollForwardQueue(String regionName, RollForwardQueue rollForwardQueue) {
        map.putIfAbsent(regionName, rollForwardQueue);
    }

    public static RollForwardQueue lookupRollForward(String regionName) {
        return map.get(regionName);
    }
    public static void deregisterRegion(String regionName) {
        map.remove(regionName);
    }
}
