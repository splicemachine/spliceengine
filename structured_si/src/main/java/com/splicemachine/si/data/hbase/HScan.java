package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.SScan;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

public class HScan implements HRead, SScan, IOperation {
    private final Scan scan;

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

    @Override
    public void setReadTimeRange(long minTimestamp, long maxTimestamp) {
        try {
            scan.setTimeRange(minTimestamp, maxTimestamp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setReadMaxVersions() {
        scan.setMaxVersions();
    }

    @Override
    public void setReadMaxVersions(int max) {
        scan.setMaxVersions(max);
    }

    @Override
    public void addFamilyToRead(byte[] family) {
        scan.addFamily(family);
    }

    @Override
    public void addFamilyToReadIfNeeded(byte[] family) {
        scan.addFamily(family);
    }
}
