package com.splicemachine.si2.data.hbase;

import com.splicemachine.si2.data.api.STable;
import org.apache.hadoop.hbase.client.HTableInterface;

public class HbTable implements STable {
    final HTableInterface table;

    public HbTable(HTableInterface table) {
        this.table = table;
    }
}
