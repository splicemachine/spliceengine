package com.splicemachine.access.client;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
@NotThreadSafe
public class HBase10ClientSideRegionScanner extends SkeletonClientSideRegionScanner{
    private final Table table;

    public HBase10ClientSideRegionScanner(Table table,FileSystem fs,Path rootDir,HTableDescriptor htd,HRegionInfo hri,Scan scan) throws IOException{
        super(table.getConfiguration(),fs,rootDir,htd,hri,scan);
        this.table = table;
    }

    @Override
    protected ResultScanner newScanner(Scan memScan) throws IOException{
        return table.getScanner(memScan);
    }
}
