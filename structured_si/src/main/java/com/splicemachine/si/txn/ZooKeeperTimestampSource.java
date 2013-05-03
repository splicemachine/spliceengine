package com.splicemachine.si.txn;

import com.splicemachine.constants.SchemaConstants;
import com.splicemachine.constants.TransactionConstants;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperTimestampSource implements TimestampSource {
    private static Logger LOG = Logger.getLogger(ZooKeeperTimestampSource.class);
    protected RecoverableZooKeeper rzk;
    private final String transactionPath;

    public ZooKeeperTimestampSource(final String transactionPath, Configuration config) {
        this.transactionPath = transactionPath;
        try {
            final CountDownLatch connSignal = new CountDownLatch(1);
            rzk = ZKUtil.connect(config, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected)
                        connSignal.countDown();
                }
            });
            connSignal.await();
            createWithParents(rzk, config.get(TransactionConstants.TRANSACTION_PATH_NAME, TransactionConstants.DEFAULT_TRANSACTION_PATH));
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException("Fatal Error connecting to ZooKeeper",e);
        } catch (IOException e) {
            throw new RuntimeException("Fatal Error creating a ZooKeeper transaction oracle",e);
        } catch (Exception e) {
            throw new RuntimeException("Fatal Error creating a ZooKeeper transaction oracle",e);
        }
    }
    @Override
    public long nextTimestamp() {
        SpliceLogUtils.trace(LOG, "Begin transaction at server and create znode for %s", transactionPath);
        String id = null;
        try {
            id = rzk.create(transactionPath + "/txn-", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            SpliceLogUtils.debug(LOG,"Begin transaction at server and create znode for transId="+id);
        } catch (KeeperException e) {
            throw new RuntimeException("Unable to create a new transaction id",e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unable to create a new transaction id",e);
        }
        return Long.parseLong(id.substring(id.length()-10, id.length()));
    }

    public static void createWithParents(RecoverableZooKeeper rzk, String znode)
            throws KeeperException {
        try {
            if(znode == null) {
                return;
            }
            rzk.create(znode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch(KeeperException.NodeExistsException nee) {
            SpliceLogUtils.debug(LOG,"znode exists during createWithParents: "+znode);
        } catch(KeeperException.NoNodeException nne) {
            createWithParents(rzk, getParent(znode));
            createWithParents(rzk, znode);
        } catch(InterruptedException ie) {
            SpliceLogUtils.error(LOG,ie);
        }
    }

    /**
     * Returns the full path of the immediate parent of the specified node.
     * @param node path to get parent of
     * @return parent of path, null if passed the root node or an invalid node
     */
    private static String getParent(String node) {
        int idx = node.lastIndexOf(SchemaConstants.PATH_DELIMITER);
        return idx <= 0 ? null : node.substring(0, idx);
    }


}
