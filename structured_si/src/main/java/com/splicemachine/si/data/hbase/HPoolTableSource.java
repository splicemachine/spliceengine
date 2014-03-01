package com.splicemachine.si.data.hbase;

import com.splicemachine.hbase.table.BetterHTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import java.io.IOException;

/**
 * Produce tables from a HBase table pool
 */
public class HPoolTableSource implements HTableSource {
    private final BetterHTablePool pool;

    public HPoolTableSource(BetterHTablePool pool) {
        this.pool = pool;
    }

    @Override
    public HTableInterface getTable(String tableName) throws IOException {
        return pool.getTable(tableName);
    }
}
