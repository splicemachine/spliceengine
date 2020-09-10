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
 */

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.regionserver.HRegion;
import splice.com.google.common.collect.Iterators;
import splice.com.google.common.collect.PeekingIterator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.mock;

public abstract class AbstractIteratorRegionScanner extends HRegion.RegionScannerImpl{
    private PeekingIterator<Set<Cell>> kvs;
    private Scan scan;
    private Filter filter;

    public AbstractIteratorRegionScanner(HRegion r, Iterator<Set<Cell>> kvs,Scan scan) throws IOException {
        r.super(scan, null, r);
        this.kvs = Iterators.peekingIterator(kvs);
        this.scan = scan;
        this.filter = scan.getFilter();
    }

    @Override public HRegionInfo getRegionInfo() { return mock(HRegionInfo.class); }

    @Override
    public boolean isFilterDone() throws IOException {
        return filter!=null && filter.filterAllRemaining();
    }

    @SuppressWarnings("LoopStatementThatDoesntLoop")
    @Override
    public boolean reseek(byte[] row) throws IOException {
        if(!kvs.hasNext()) return false;
        while(kvs.hasNext()){
            Set<Cell> next = kvs.peek();
            if(next.size()<0){
                kvs.next();  //throw empty rows away
                continue;
            }
            for(Cell kv: next){
                if(Bytes.equals(kv.getRowArray(),kv.getRowOffset(),kv.getRowLength(),row,0, row.length))
                    return true;
                else{
                    kvs.next();
                    break;
                }
            }
        }
        return false;
    }

    @Override public long getMvccReadPoint() { return 0; }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return next(result);
    }

//    @Override
    public boolean nextRaw(List<Cell> result, int limit) throws IOException {
        return next(result);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        OUTER: while(kvs.hasNext()){
            Set<Cell> next = kvs.next();
            List<Cell> toAdd = new ArrayList<>(next.size());
            for(Cell kv:next){
                if(!containedInScan(kv)) continue; //skip KVs we aren't interested in
                if(filter!=null){
                    Filter.ReturnCode retCode = filter.filterKeyValue(kv);
                    switch(retCode){
                        case INCLUDE:
                        case INCLUDE_AND_NEXT_COL:
                            toAdd.add(kv);
                            break;
                        case SKIP:
                        case NEXT_COL:
                            break;
                        case NEXT_ROW:
                            continue OUTER;
                        case SEEK_NEXT_USING_HINT:
                            throw new UnsupportedOperationException("DON'T USE THIS");
                    }
                }
            }
            if(filter!=null){
                if(filter.filterRow()) continue;

                filter.filterRowCells(toAdd);
                filter.reset();
            }
            if(toAdd.size()>0){
                results.addAll(toAdd);
                return true;
            }
        }
        return false;
    }

    private boolean containedInScan(Cell kv) {
        byte[] rowArray = kv.getRowArray();
        int rowOffset = kv.getRowOffset();
        int rowLength = kv.getRowLength();
        if(Bytes.compareTo(scan.getStartRow(),0,scan.getStartRow().length,rowArray,rowOffset,rowLength)>0) return false;
        if(Bytes.compareTo(scan.getStopRow(),0,scan.getStopRow().length,rowArray,rowOffset,rowLength)<=0) return false;
        byte[] family = CellUtil.cloneFamily(kv);
        Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
        if(familyMap.size()<=0) return true;

        if(!familyMap.containsKey(family)) return false;
        NavigableSet<byte[]> qualifiersToFetch = familyMap.get(family);
        if(qualifiersToFetch.size()<=0) return true;
        return qualifiersToFetch.contains(CellUtil.cloneQualifier(kv));
    }



    @Override
    public void close() {
        //no-op
    }

//    @Override
    public boolean next(List<Cell> result, int limit) throws IOException {
        return next(result);
    }

    @Override
    public long getMaxResultSize() {
        return Long.MAX_VALUE;
    }
}

