package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.STable;
import org.apache.hadoop.hbase.client.HTableInterface;

public class HbTable implements STable {
    final HTableInterface table;

    public HbTable(HTableInterface table) {
        this.table = table;
    }
}
