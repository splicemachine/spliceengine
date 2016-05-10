package com.splicemachine.access.client;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
@NotThreadSafe
public class HBase10ClientSideRegionScanner extends SkeletonClientSideRegionScanner{
    private final Table table;

    public HBase10ClientSideRegionScanner(Table table,
                                          FileSystem fs,
                                          Path rootDir,
                                          HTableDescriptor htd,
                                          HRegionInfo hri,
                                          Scan scan, String hostAndPort) throws IOException{
        super(table.getConfiguration(),fs,rootDir,htd,hri,scan,hostAndPort);
        this.table = table;
        updateScanner();
    }

    @Override
    protected ResultScanner newScanner(Scan memScan) throws IOException{
        return table.getScanner(memScan);
    }

    public int getBatch() {
        return 0;
    }

    public boolean nextRaw(List<Cell> cells, ScannerContext scannerContext) throws IOException {
        return false;
    }

    public boolean next(List<Cell> cells, ScannerContext scannerContext) throws IOException {
        return false;
    }
}
