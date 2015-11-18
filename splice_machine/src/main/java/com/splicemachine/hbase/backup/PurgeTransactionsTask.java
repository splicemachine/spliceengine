package com.splicemachine.hbase.backup;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 *
 *
 */
public class PurgeTransactionsTask {
    private static Logger LOG = Logger.getLogger(PurgeTransactionsTask.class);

    private static final long serialVersionUID = 5l;
    private long backupTimestamp;
    HRegion region;

    public PurgeTransactionsTask() {
    }

    public PurgeTransactionsTask(long backupTimestamp, String jobId) {
        this.backupTimestamp = backupTimestamp;
    }

    public void doExecute() throws ExecutionException, InterruptedException {
        try {
            RegionTxnPurger txnPurger = new RegionTxnPurger(region);
            txnPurger.rollbackTransactionsAfter(backupTimestamp);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, "Couldn't purge transactions from region " + region, e);
            throw new ExecutionException("Failed purge of transactions of region " + region, e);
        }
    }
}

