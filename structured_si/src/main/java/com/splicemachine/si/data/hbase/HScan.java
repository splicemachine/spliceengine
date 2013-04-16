package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.SScan;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Scan;

public class HScan implements SScan, IOperation {
    final Scan scan;

    public HScan(Scan scan) {
        this.scan = scan;
    }

    @Override
    public OperationWithAttributes getOperation() {
        return scan;
    }

    public Scan getScan() {
        return scan;
    }
}
