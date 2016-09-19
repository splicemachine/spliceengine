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
import org.apache.zookeeper.KeeperException;
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

    public PlaceholderTask() { }

    public PlaceholderTask(String path, int timeout) {
        this.path = path;
        this.timeout = timeout;
    }

    @Override
    public Object call(Object o) throws Exception {
        latch = new CountDownLatch(1);
        try{
            ZkUtils.getRecoverableZooKeeper().getData(path,this,null);
        }catch(KeeperException ke){
            /*
             * If the exception is a NONODE type, then we encountered a situation where we were terminated
             * before we could begin execution. This is cool, we should just skip to the exception throwing
             * business and not worry about it. If it isn't of that type, though, we need to inform the world
             * of a potential problem.
             *
             * We do that by just counting down the latch, so that subsequent await() calls return immediately.
             * We don't have to worry about double count downs for two reasons: The first is that we will never
             * have attached a watcher to the node since the getData() call failed. The second is that if the
             * latch is already at 0, then calling countDown() is a no-op and doesn't break anything, so yippee
             * for us!
             */
            if(!ke.code().equals(KeeperException.Code.NONODE))
                throw ke;
            else latch.countDown(); //just count down the latch, so that await() returns immediately
        }
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
