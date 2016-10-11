/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
