package com.splicemachine.hbase.table;

import java.util.Random;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

public class SpliceHTableUtil {
	public static int RETRY_BACKOFF[] = { 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12};
	public static Random random = new Random();

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
        if(tryNum>= RETRY_BACKOFF.length)
            return RETRY_BACKOFF[HConstants.RETRY_BACKOFF.length-1]*pause;
        else if (tryNum < 6 )
            return RETRY_BACKOFF[tryNum]*Math.round(pause*random.nextDouble());
        else 
            return RETRY_BACKOFF[tryNum]*pause;        	
    }
}
