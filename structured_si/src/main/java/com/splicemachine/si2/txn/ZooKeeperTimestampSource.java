package com.splicemachine.si2.txn;

import com.splicemachine.constants.TxnConstants;
import com.splicemachine.si.utils.SIUtils;
import com.splicemachine.si2.si.api.TimestampSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperTimestampSource implements TimestampSource {
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
            HBaseAdmin admin = new HBaseAdmin(new Configuration());
            SIUtils.createWithParents(rzk, admin.getConfiguration().get(TxnConstants.TRANSACTION_PATH_NAME, TxnConstants.DEFAULT_TRANSACTION_PATH));
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public long nextTimestamp() {
        return SIUtils.createIncreasingTimestamp(transactionPath, rzk);
    }
}
