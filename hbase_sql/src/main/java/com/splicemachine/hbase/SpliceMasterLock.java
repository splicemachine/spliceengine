/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.hbase;

import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.util.concurrent.Semaphore;

/**
 * Created by dgomezferro on 25/08/2017.
 */
public class SpliceMasterLock implements Watcher {
    private final String path;
    private final String parent;
    private final RecoverableZooKeeper zk;
    private final Semaphore semaphore;
    private boolean acquired;

    public SpliceMasterLock(String parentPath, String path, RecoverableZooKeeper zooKeeper) {
        this.path = path;
        this.parent = parentPath;
        this.zk = zooKeeper;
        this.semaphore = new Semaphore(0);
    }

    public void acquire() throws InterruptedException, KeeperException {
        ZkUtils.recursiveSafeCreate(parent,new byte[]{},ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        while (true) {
            try {
                zk.getData(path, this, null);
                // node exists, wait for it to go away
                semaphore.acquire();
                continue;
            } catch (KeeperException e) {
                if (e.code().equals(KeeperException.Code.NONODE)) {
                    // good, we'll create it
                } else {
                    throw e;
                }
            }
            // node didn't exist, we'll try to create it
            try {
                zk.create(path, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                acquired = true;
                return;
            } catch (KeeperException e) {
                if (e.code().equals(KeeperException.Code.NODEEXISTS)) {
                    // whoops, somebody was quicker, retry
                    continue;
                } else {
                    throw e;
                }
            }
        }
    }

    public void release() throws KeeperException, InterruptedException {
        zk.delete(path, -1);
        acquired = false;
    }

    public boolean isAcquired() {
        return acquired;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        // something changed, retry
        semaphore.release();
    }
}
