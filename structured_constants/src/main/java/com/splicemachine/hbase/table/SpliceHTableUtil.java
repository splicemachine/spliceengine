package com.splicemachine.hbase.table;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

public class SpliceHTableUtil {

    public static HTable toHTable(HTableInterface table) {
        if (table instanceof HTable)
            return (HTable) table;
        else if (table instanceof BetterHTablePool.ReturningHTable) {
            return ((BetterHTablePool.ReturningHTable) table).getDelegate();
        } else {
            return null;
        }
    }

    public static long getWaitTime(int tryNum,long pause) {
        long retryWait;
        if(tryNum>= HConstants.RETRY_BACKOFF.length)
            retryWait = HConstants.RETRY_BACKOFF[HConstants.RETRY_BACKOFF.length-1];
        else
            retryWait = HConstants.RETRY_BACKOFF[tryNum];
        return retryWait*pause;
    }
}
