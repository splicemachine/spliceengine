package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.SGet;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;

public class HGet implements SGet, IOperation {
    final Get get;

    public HGet(Get get) {
        this.get = get;
    }

    public Get getGet() {
        return get;
    }

    @Override
    public OperationWithAttributes getOperation() {
        return get;
    }
}
