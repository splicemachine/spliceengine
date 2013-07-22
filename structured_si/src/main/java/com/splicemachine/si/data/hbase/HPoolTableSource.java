package com.splicemachine.si.data.hbase;

import com.splicemachine.hbase.table.BetterHTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;

/**
 * Produce tables from a HBase table pool
 */
public class HPoolTableSource implements HTableSource {
    private final BetterHTablePool pool;
//    private final HTablePool pool;

    public HPoolTableSource(BetterHTablePool pool) {
//        public HPoolTableSource(HTablePool pool) {
        this.pool = pool;
    }

    @Override
    public HTableInterface getTable(String tableName) throws IOException {
        return pool.getTable(tableName);
//        return pool.getTable(HDataLib.convertToBytes(tableName, String.class));
    }
}
