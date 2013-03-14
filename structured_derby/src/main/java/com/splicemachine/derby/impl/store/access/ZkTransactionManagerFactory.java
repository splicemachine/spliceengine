package com.splicemachine.derby.impl.store.access;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.ITransactionManager;
import com.splicemachine.constants.ITransactionManagerFactory;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.ZkUtils;
import com.splicemachine.hbase.txn.ZkTransactionManager;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ZkTransactionManagerFactory implements ITransactionManagerFactory {
    private static Logger LOG = Logger.getLogger(ZkTransactionManagerFactory.class);

    @Override
    public ITransactionManager newTransactionManager() throws IOException {
        return new ZkTransactionManager(SpliceUtils.getTransactionPath(), ZkUtils.getZooKeeperWatcher(),
                ZkUtils.getRecoverableZooKeeper());
    }

    @Override
    public void init() {
        if (LOG.isDebugEnabled())
            LOG.debug("SpliceTransactionFactory initZookeeper");
        HBaseAdmin admin = null;
        try {
            @SuppressWarnings("resource")
            Configuration config = HBaseConfiguration.create();
            admin = new HBaseAdmin(config);
            if (!admin.tableExists(TxnConstants.TRANSACTION_LOG_TABLE_BYTES)) {
                HTableDescriptor desc = new HTableDescriptor(TxnConstants.TRANSACTION_LOG_TABLE_BYTES);
                desc.addFamily(new HColumnDescriptor(HBaseConstants.DEFAULT_FAMILY.getBytes(),
                        HBaseConstants.DEFAULT_VERSIONS,
                        HBaseConstants.DEFAULT_COMPRESSION,
//						config.get(HBaseConstants.TABLE_COMPRESSION,HBaseConstants.DEFAULT_COMPRESSION),
                        HBaseConstants.DEFAULT_IN_MEMORY,
                        HBaseConstants.DEFAULT_BLOCKCACHE,
                        HBaseConstants.DEFAULT_TTL,
                        HBaseConstants.DEFAULT_BLOOMFILTER));
                desc.addFamily(new HColumnDescriptor(TxnConstants.DEFAULT_FAMILY));
                admin.createTable(desc);
            }
            if (!admin.tableExists(TxnConstants.TEMP_TABLE)) {
                HTableDescriptor td = SpliceUtils.generateDefaultDescriptor(TxnConstants.TEMP_TABLE);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, TxnConstants.TEMP_TABLE + " created");
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
