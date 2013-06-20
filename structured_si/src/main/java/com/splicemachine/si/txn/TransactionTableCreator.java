package com.splicemachine.si.txn;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

public class TransactionTableCreator extends SIConstants {
    static final Logger LOG = Logger.getLogger(TransactionTableCreator.class);

    @SuppressWarnings("deprecation")
	public static void createTransactionTableIfNeeded(Configuration configuration) {
        try {
            @SuppressWarnings("resource")
            HBaseAdmin admin = new HBaseAdmin(configuration);
            if (!admin.tableExists(TRANSACTION_TABLE_BYTES)) {
                HTableDescriptor desc = new HTableDescriptor(TRANSACTION_TABLE_BYTES);
                desc.addFamily(new HColumnDescriptor(DEFAULT_FAMILY.getBytes(),
                        5,
                        compression,
                        DEFAULT_IN_MEMORY,
                        DEFAULT_BLOCKCACHE,
                        Integer.MAX_VALUE,
                        DEFAULT_BLOOMFILTER));
                desc.addFamily(new HColumnDescriptor(DEFAULT_FAMILY));
                admin.createTable(desc);
            }
        } catch (Exception e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
        }
    }

}
