package com.splicemachine.si.txn;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.TransactionConstants;
import com.splicemachine.si.api.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

public class TransactionTableCreator {
    static final Logger LOG = Logger.getLogger(TransactionTableCreator.class);

    public static void createTransactionTableIfNeeded(Configuration configuration) {
        try {
            @SuppressWarnings("resource")
            HBaseAdmin admin = new HBaseAdmin(configuration);
            if (!admin.tableExists(TransactionConstants.TRANSACTION_TABLE_BYTES)) {
                HTableDescriptor desc = new HTableDescriptor(TransactionConstants.TRANSACTION_TABLE_BYTES);
                desc.addFamily(new HColumnDescriptor(HBaseConstants.DEFAULT_FAMILY.getBytes(),
                        5,
                        admin.getConfiguration().get(HBaseConstants.TABLE_COMPRESSION, HBaseConstants.DEFAULT_COMPRESSION),
                        HBaseConstants.DEFAULT_IN_MEMORY,
                        HBaseConstants.DEFAULT_BLOCKCACHE,
                        Integer.MAX_VALUE,
                        HBaseConstants.DEFAULT_BLOOMFILTER));
                desc.addFamily(new HColumnDescriptor(HBaseConstants.DEFAULT_FAMILY));
                desc.addFamily(new HColumnDescriptor(SIConstants.SNAPSHOT_ISOLATION_CHILDREN_FAMILY));

                admin.createTable(desc);
            }
        } catch (Exception e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
        }
    }

    public static HColumnDescriptor createTransactionFamily() {
        final HColumnDescriptor siFamily = new HColumnDescriptor(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES);
        siFamily.setMaxVersions(Integer.MAX_VALUE);
        siFamily.setTimeToLive(Integer.MAX_VALUE);
        return siFamily;
    }
}
