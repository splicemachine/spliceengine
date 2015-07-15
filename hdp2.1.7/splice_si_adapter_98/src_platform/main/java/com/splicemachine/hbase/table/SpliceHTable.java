package com.splicemachine.hbase.table;

import com.splicemachine.hbase.SpliceBaseHTable;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * @author Scott Fines
 *         Created on: 10/23/13
 */
public class SpliceHTable extends SpliceBaseHTable {

    public SpliceHTable(byte[] tableName, Configuration configuration, boolean retryAutomatically) throws IOException {
        super(tableName, configuration, retryAutomatically);
    }

    public SpliceHTable(byte[] tableName, HConnection connection, ExecutorService pool,
                        RegionCache regionCache) throws IOException {
        super(tableName, connection, pool, regionCache);
    }

}