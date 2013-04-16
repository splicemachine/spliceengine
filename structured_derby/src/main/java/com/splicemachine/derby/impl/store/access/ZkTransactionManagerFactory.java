package com.splicemachine.derby.impl.store.access;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.ZkUtils;
import com.splicemachine.hbase.txn.TransactionManager;
import com.splicemachine.hbase.txn.ZkTransactionManager;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ZkTransactionManagerFactory {
    private static Logger LOG = Logger.getLogger(ZkTransactionManagerFactory.class);

    public TransactionManager newTransactionManager() throws IOException {
        return new ZkTransactionManager(SpliceUtils.getTransactionPath(), ZkUtils.getZooKeeperWatcher(),
                ZkUtils.getRecoverableZooKeeper());
    }

    public void init() {
        if (LOG.isDebugEnabled())
            LOG.debug("SpliceTransactionFactory initZookeeper");
        HBaseAdmin admin = null;
        try {
            @SuppressWarnings("resource")
            Configuration config = HBaseConfiguration.create();
            admin = new HBaseAdmin(config);
            if (!admin.tableExists(HBaseConstants.TEMP_TABLE)) {
                HTableDescriptor td = SpliceUtils.generateDefaultDescriptor(HBaseConstants.TEMP_TABLE);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, HBaseConstants.TEMP_TABLE + " created");
            }
        } catch (MasterNotRunningException e) {
            SpliceLogUtils.error(LOG, e);
        } catch (ZooKeeperConnectionException e) {
            SpliceLogUtils.error(LOG, e);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (Exception e) {
                    SpliceLogUtils.error(LOG, e);
                }
            }
        }
    }

}
