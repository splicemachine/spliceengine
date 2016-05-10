package com.splicemachine.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

import java.io.IOException;
import java.util.List;

/**
 * Created by jleach on 5/4/16.
 */
public class StubInternalScanner extends Scan implements InternalScanner {

    @Override
    public boolean next(List<Cell> results) throws IOException {
        return false;
    }

    @Override
    public boolean next(List<Cell> cells, ScannerContext scannerContext) throws IOException {
        return false;
    }

    @Override
    public void close() throws IOException {

    }
}