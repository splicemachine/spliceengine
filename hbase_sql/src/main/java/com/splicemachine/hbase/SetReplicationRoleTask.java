package com.splicemachine.hbase;

import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Created by jyuan on 12/9/19.
 */
public class SetReplicationRoleTask implements Callable<Void> {

    public Void call() {

        try {
            String replicationPath = ReplicationUtils.getReplicationPath();
            byte[] status = ZkUtils.getData(replicationPath);
            if (Bytes.compareTo(status, HBaseConfiguration.REPLICATION_NONE) != 0) {
                while (DatabaseLifecycleManager.manager().getState() != DatabaseLifecycleManager.State.RUNNING) {
                    Thread.sleep(100);
                }
                if (Bytes.compareTo(status, HBaseConfiguration.REPLICATION_MASTER) == 0) {
                    ReplicationUtils.setReplicationRoleLocal("MASTER");
                }
                else if (Bytes.compareTo(status, HBaseConfiguration.REPLICATION_SLAVE) == 0) {
                    ReplicationUtils.setReplicationRoleLocal("SLAVE");
                }
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
