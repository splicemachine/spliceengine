package com.splicemachine.si.data.hbase.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

import java.io.IOException;
import java.util.List;

public class DummyScanner implements InternalScanner {
    public static DummyScanner INSTANCE = new DummyScanner();

    private DummyScanner() {};

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return false;
    }

    @Override
    public void close() throws IOException {

    }
}
