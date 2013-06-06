package com.splicemachine.si.txn;

import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

public class ZooKeeperTimestampSource implements TimestampSource {
    private static Logger LOG = Logger.getLogger(ZooKeeperTimestampSource.class);
    protected RecoverableZooKeeper rzk;
    private final String transactionPath;

    public ZooKeeperTimestampSource(final String transactionPath) {
        this.transactionPath = transactionPath;
    }
    @Override
    public long nextTimestamp() {
        SpliceLogUtils.trace(LOG, "Begin transaction at server and create znode for %s", transactionPath);
        String id = null;
        try {
            id = ZkUtils.create(transactionPath + "/txn-", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            SpliceLogUtils.debug(LOG,"Begin transaction at server and create znode for transId=%s",id);
        } catch (KeeperException e) {
            throw new RuntimeException("Unable to create a new transaction id",e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unable to create a new transaction id",e);
        }
        return Long.parseLong(id.substring(id.length()-10, id.length()));
    }

    public static void createWithParents(RecoverableZooKeeper rzk, String znode)
            throws KeeperException, InterruptedException {
            if(znode == null)
                return;
            ZkUtils.recursiveSafeCreate(znode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

}
