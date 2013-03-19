package com.splicemachine.si2.data.hbase;

import com.splicemachine.si2.data.api.SDataLib;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;

/**
 * Produce tables from a HBase table pool
 */
public class HPoolTableSource implements HTableSource {
    private final HTablePool pool;
    private final HDataLib dataLib = new HDataLib();

    public HPoolTableSource(HTablePool pool) {
        this.pool = pool;
    }

    @Override
    public HTableInterface getTable(String tableName) throws IOException {
        return pool.getTable(dataLib.encode(tableName));
    }
}
