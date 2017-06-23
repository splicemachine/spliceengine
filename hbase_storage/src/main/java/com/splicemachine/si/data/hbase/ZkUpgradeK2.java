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

package com.splicemachine.si.data.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.timestamp.api.TimestampSource;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZkUpgradeK2 implements Watcher {
    private static final Logger LOG = Logger.getLogger(ZkUpgradeK2.class);
    private static final String K2_NODE = "/isK2";
    private final String path;
    private long oldTxns;
    private boolean init = false;
    private final String spliceRootPath;

    public ZkUpgradeK2(String spliceRootPath){
        this.spliceRootPath = spliceRootPath;
        path = spliceRootPath+K2_NODE;
    }

    public boolean upgrading() throws IOException {
        try {
            return ZkUtils.getData(path) != null;
        } catch (IOException e) {
            if (e.getCause() instanceof KeeperException) {
                KeeperException ke = (KeeperException) e.getCause();
                if (ke.code() == KeeperException.Code.NONODE) {
                    return false;
                }
            }
            throw e;
        }
    }

    public void upgrade(long timestamp) throws IOException {
        oldTxns = timestamp;
        try {
            ZkUtils.create(spliceRootPath + HConfiguration.OLD_TRANSACTIONS_NODE, Bytes.toBytes(oldTxns), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            ZkUtils.delete(path);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private Object latch = new Object();

    public synchronized long getOldTransactions() throws IOException {
        if (init)
            return oldTxns;
        init = true;
        try {
            long start = System.currentTimeMillis();
            try {
                synchronized (latch) {
                    while (ZkUtils.getRecoverableZooKeeper().exists(path, this) != null) {
                        long diff = System.currentTimeMillis() - start;
                        LOG.info("Waiting for master to clear node: " + path);
                        if (diff > 30 * 60 * 1000) {
                            LOG.error("Node " + path + " hasn't been cleared yet, master didn't come up?");
                            throw new IOException(path + " hasn't been cleared after 30 minutes");
                        }
                        latch.wait(30000);
                    }
                }
            } catch (Exception ie) {
                throw new IOException(ie);
            }
            oldTxns = Bytes.toLong(ZkUtils.getData(spliceRootPath + HConfiguration.OLD_TRANSACTIONS_NODE));

            LOG.info("Read old transactions threshold: " + oldTxns);
        } catch (IOException e) {
            if (e.getCause() instanceof KeeperException) {
                KeeperException ke = (KeeperException) e.getCause();
                if (ke.code() == KeeperException.Code.NONODE) {
                    oldTxns = 0;
                    return oldTxns;
                }
            }
            throw e;
        }
        return oldTxns;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        synchronized (latch) {
            latch.notifyAll();
        }
    }
}
