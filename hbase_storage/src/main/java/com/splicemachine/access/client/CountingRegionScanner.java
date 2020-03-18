/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.access.client;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by dgomezferro on 06/10/2017.
 */
public class CountingRegionScanner implements RegionScanner {
    private static final Logger LOG = Logger.getLogger(CountingRegionScanner.class);

    private final RegionScanner delegate;
    private final Scan scan;
    private final HRegion region;
    private long rows;

    public CountingRegionScanner(RegionScanner delegate, HRegion region, Scan scan) {
        this.delegate = delegate;
        this.scan = scan;
        this.region = region;
    }
    @Override
    public HRegionInfo getRegionInfo() {
        return (HRegionInfo) delegate.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() throws IOException {
        return delegate.isFilterDone();
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        return delegate.reseek(row);
    }

    @Override
    public long getMaxResultSize() {
        return delegate.getMaxResultSize();
    }

    @Override
    public long getMvccReadPoint() {
        return delegate.getMvccReadPoint();
    }

    @Override
    public int getBatch() {
        return delegate.getBatch();
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        rows++;
        return delegate.nextRaw(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return delegate.nextRaw(result, scannerContext);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        return delegate.nextRaw(results);
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return delegate.nextRaw(result, scannerContext);
    }

    @Override
    public void close() throws IOException {
        if (LOG.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (Store s : region.getStores()) {
                sb.append(s.getStorefiles());
            }
            LOG.info("Closing RegionScanner with scan " + scan + " after " + rows + " rows with storefiles" + sb.toString());
        }

        delegate.close();
    }
}
