package com.splicemachine.si.coprocessors;

import com.splicemachine.si.impl.RollForwardQueue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class RollForwardQueueMap {
    private static Map<String, RollForwardQueue<byte[], ByteBuffer>> map = new HashMap<String, RollForwardQueue<byte[], ByteBuffer>>();

    public static synchronized void registerRollForwardQueue(String tableName, RollForwardQueue<byte[], ByteBuffer> rollForwardQueue) {
        map.put(tableName, rollForwardQueue);
    }

    public static synchronized RollForwardQueue<byte[], ByteBuffer> lookupRollForwardQueue(String tableName) {
        return map.get(tableName);
    }
}
