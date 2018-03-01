/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
