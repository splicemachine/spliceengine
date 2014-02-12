package com.splicemachine.hbase.table;

import java.util.Random;

import com.splicemachine.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

public class SpliceHTableUtil {
	public static int RETRY_BACKOFF[] = { 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12, 1, 1, 4, 4, 4, 8, 12};

    public static HTable toHTable(HTableInterface table) {
        if (table instanceof HTable)
            return (HTable) table;
        else if (table instanceof BetterHTablePool.ReturningHTable) {
            return ((BetterHTablePool.ReturningHTable) table).getDelegate();
        } else {
            return null;
        }
    }

		public static long getWaitTime(int tryNum,long pause){
				//get a random wait time between 100 ms (the minimum) and the pause time for this attempt
				return ThreadLocalRandom.current().nextLong(100,Math.max(101,getMaxWaitTime(tryNum,pause)));
		}

		private static long getMaxWaitTime(int tryNum,long pause) {
        if(tryNum>= RETRY_BACKOFF.length)
            return RETRY_BACKOFF[HConstants.RETRY_BACKOFF.length-1]*pause;
				return RETRY_BACKOFF[tryNum]*pause;
    }
}
