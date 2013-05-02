package com.splicemachine.si.txn;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.SpliceConstants;
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
            if (!admin.tableExists(SpliceConstants.TRANSACTION_TABLE_BYTES)) {
                HTableDescriptor desc = new HTableDescriptor(SpliceConstants.TRANSACTION_TABLE_BYTES);
                desc.addFamily(new HColumnDescriptor(SpliceConstants.DEFAULT_FAMILY.getBytes(),
                        5,
                        admin.getConfiguration().get(SpliceConstants.TABLE_COMPRESSION, SpliceConstants.DEFAULT_COMPRESSION),
                        SpliceConstants.DEFAULT_IN_MEMORY,
                        SpliceConstants.DEFAULT_BLOCKCACHE,
                        Integer.MAX_VALUE,
                        SpliceConstants.DEFAULT_BLOOMFILTER));
                desc.addFamily(new HColumnDescriptor(SpliceConstants.DEFAULT_FAMILY));
                desc.addFamily(new HColumnDescriptor(SIConstants.SNAPSHOT_ISOLATION_CHILDREN_FAMILY));

                admin.createTable(desc);
            }
        } catch (Exception e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
        }
    }

}
