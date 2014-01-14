package com.splicemachine.si.txn;

import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

public class ZooKeeperTimestampSource implements TimestampSource {
    private static Logger LOG = Logger.getLogger(ZooKeeperTimestampSource.class);
    protected RecoverableZooKeeper rzk;
    private final String transactionPath;

    public ZooKeeperTimestampSource(final String transactionPath) {
        this(transactionPath,ZkUtils.getRecoverableZooKeeper());
    }

    public ZooKeeperTimestampSource(final String transactionPath,RecoverableZooKeeper rzk) {
        this.transactionPath = transactionPath;
        this.rzk = rzk;
    }

    @Override
    public long nextTimestamp() {
    	if (LOG.isTraceEnabled())
    			SpliceLogUtils.trace(LOG, "Begin transaction at server and create znode for %s", transactionPath);
        String id;
        try {
            id = rzk.create(transactionPath + "/txn-", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            SpliceLogUtils.debug(LOG,"Begin transaction at server and create znode for transId=%s",id);
            deleteAsync(id);
        } catch (KeeperException e) {
            throw new RuntimeException("Unable to create a new transaction id",e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unable to create a new transaction id",e);
        }
        return Long.parseLong(id.substring(id.length()-10, id.length()));
    }

    private void deleteAsync(final String id) {
        rzk.getZooKeeper().delete(id,-1,new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int i, String s, Object o) {
                if(LOG.isTraceEnabled())
                    LOG.trace("Transaction node "+ id+" cleaned up");
            }
        },this);
    }

    @Override
    public void rememberTimestamp(long timestamp) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public long retrieveTimestamp() {
        throw new RuntimeException("not implemented");
    }
}
