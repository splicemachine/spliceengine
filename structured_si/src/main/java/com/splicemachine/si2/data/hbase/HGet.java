package com.splicemachine.si2.data.hbase;

import com.splicemachine.si2.data.api.SGet;
import org.apache.hadoop.hbase.client.Get;

public class HGet implements SGet {
    final Get get;

    public HGet(Get get) {
        this.get = get;
    }
}
