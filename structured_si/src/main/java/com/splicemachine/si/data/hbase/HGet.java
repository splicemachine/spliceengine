package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.SGet;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;

import java.io.IOException;

public class HGet implements HRead, SGet, IOperation {
    private final Get get;

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

    @Override
    public void setReadTimeRange(long minTimestamp, long maxTimestamp) {
        try {
            get.setTimeRange(minTimestamp, maxTimestamp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setReadMaxVersions() {
        get.setMaxVersions();
    }

    @Override
    public void setReadMaxVersions(int max) {
        try {
            get.setMaxVersions(max);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addFamilyToRead(byte[] family) {
        get.addFamily(family);
    }

    @Override
    public void addFamilyToReadIfNeeded(byte[] family) {
        if (get.hasFamilies()) {
            get.addFamily(family);
        }
    }
}
