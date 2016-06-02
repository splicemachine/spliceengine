package com.splicemachine.hbase.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/21/15
 */
public class IteratorRegionScanner extends AbstractIteratorRegionScanner{
    public IteratorRegionScanner(Iterator<Set<Cell>> kvs,Scan scan){
        super(kvs,scan);
    }


    @Override
    public int getBatch() {
        return 0;
    }

    @Override
    public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result);
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result);
    }
}
