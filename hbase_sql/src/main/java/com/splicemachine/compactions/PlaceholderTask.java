package com.splicemachine.compactions;

import com.splicemachine.hbase.ZkUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.spark.api.java.function.Function;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by dgomezferro on 6/17/16.
 *
 * This function makes a Spark task block until notified to reserve this task
 */
public class PlaceholderTask implements Function<Object, Object>, Watcher {
    String path;
    int timeout;
    volatile CountDownLatch latch;

    public PlaceholderTask() {
    }

    public PlaceholderTask(String path, int timeout) {
        this.path = path;
        this.timeout = timeout;
    }

    @Override
    public Object call(Object o) throws Exception {
        latch = new CountDownLatch(1);
        ZkUtils.getRecoverableZooKeeper().getData(path, this, null);
        if (latch.await(timeout * 2, TimeUnit.SECONDS)) {
            throw new IgnoreThisException("Expected exception, ignore");
        } else {
            throw new IllegalStateException("Booking task timed out before notification");
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType().equals(Event.EventType.NodeDeleted)) {
            latch.countDown();
        }
    }
}
