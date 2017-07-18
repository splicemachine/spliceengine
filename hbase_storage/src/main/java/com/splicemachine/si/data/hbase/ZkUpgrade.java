package com.splicemachine.si.data.hbase;


import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Created by jyuan on 7/17/17.
 */
public class ZkUpgrade {
    private static final Logger LOG = Logger.getLogger(ZkUpgrade.class);
    private static final String OLD_TRANSACTIONS_NODE = "/transactions/v1transactions";

    public static long getOldTransactions(SConfiguration conf) throws IOException {
        try {
            String spliceRootPath = conf.getSpliceRootPath();
            byte[] bytes = ZkUtils.getData(spliceRootPath + OLD_TRANSACTIONS_NODE);
            long txn =  bytes != null ? Bytes.toLong(bytes) : 0;
            SpliceLogUtils.info(LOG, "Max V1 format transaction = %d", txn);

            return txn;
        } catch (IOException e) {
            Throwable t = e.getCause();
            if (t instanceof KeeperException.NoNodeException) {
                SpliceLogUtils.info(LOG, "No transaction is encoded in V1 format");
                return 0;
            }
            else throw e;
        }
    }
}
