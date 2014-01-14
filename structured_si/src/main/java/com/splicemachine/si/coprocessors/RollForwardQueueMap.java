package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.RollForwardQueue;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RollForwardQueueMap {
    private static ConcurrentMap<String,RollForwardQueueHolder> map  = new NonBlockingHashMap<String, RollForwardQueueHolder>();
//    private static Map<String, RollForwardQueue<byte[], ByteBuffer>> map = new HashMap<String, RollForwardQueue<byte[], ByteBuffer>>();

    public static void registerRollForwardQueue(String tableName, RollForwardQueue<byte[], ByteBuffer> rollForwardQueue) {
        RollForwardQueueHolder holder = new RollForwardQueueHolder(rollForwardQueue);
        map.putIfAbsent(tableName, holder);
    }

    public static RollForwardQueue<byte[], ByteBuffer> lookupRollForwardQueue(String tableName) {
        RollForwardQueueHolder holder = map.get(tableName);
        if(holder==null) return null;
        return holder.queue;
    }

    public static void deregisterRegion(String tableName) {

        RollForwardQueueHolder holder = map.get(tableName);
        if(holder==null) return;

        int count = holder.refCount.decrementAndGet();
        if(count<=0){
            //we've closed all the regions for this table, so remove the queue from the map to prevent memory leaks
            map.remove(tableName,holder);
            holder.queue.stop();
        }
    }


    private static class RollForwardQueueHolder{
        private final AtomicInteger refCount = new AtomicInteger(0);
        private final RollForwardQueue<byte[],ByteBuffer> queue;

        private RollForwardQueueHolder(RollForwardQueue<byte[], ByteBuffer> queue) {
            this.queue = queue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RollForwardQueueHolder)) return false;

            RollForwardQueueHolder that = (RollForwardQueueHolder) o;

            return queue.equals(that.queue);

        }

        @Override
        public int hashCode() {
            return queue.hashCode();
        }
    }

}
