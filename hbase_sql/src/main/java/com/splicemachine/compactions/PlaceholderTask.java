/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.compactions;

import com.splicemachine.hbase.ZkUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.log4j.Logger;
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
    private static final Logger LOG = Logger.getLogger(PlaceholderTask.class);
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
        if (!latch.await(timeout * 2, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Booking task timed out before notification");
        }
        return null;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType().equals(Event.EventType.NodeDeleted)) {
            latch.countDown();
        }
    }
}
