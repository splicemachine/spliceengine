package com.splicemachine.hbase.table;

import com.splicemachine.concurrent.ThreadLocalRandom;
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

		public static long getWaitTime(int tryNum,long pause){
			return ThreadLocalRandom.current().nextLong(100, pause<=100?pause+100:pause);
		}

}
